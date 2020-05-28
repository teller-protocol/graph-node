use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::Deref;
use std::rc::Rc;
use std::time::Instant;

use semver::Version;
use wasmtime::{Memory, Trap};

use crate::host_exports;
use crate::mapping::MappingContext;
use ethabi::LogParam;
use graph::components::ethereum::*;
use graph::data::store;
use graph::prelude::*;
use web3::types::{Log, Transaction, U256};

use crate::asc_abi::asc_ptr::*;
use crate::asc_abi::class::*;
use crate::asc_abi::*;
use crate::host_exports::HostExports;
use crate::mapping::ValidModule;
use crate::UnresolvedContractCall;

mod into_wasm_ret;
use into_wasm_ret::IntoWasmRet;

#[cfg(test)]
mod test;

pub(crate) struct WasmiModule {
    pub module: wasmtime::Instance,
    memory: Memory,
    memory_allocate: Box<dyn Fn(i32) -> Result<i32, Trap>>,

    pub ctx: MappingContext,
    pub(crate) valid_module: Arc<ValidModule>,
    pub(crate) host_metrics: Arc<HostMetrics>,

    // First free byte in the current arena.
    arena_start_ptr: i32,

    // Number of free bytes starting from `arena_start_ptr`.
    arena_free_size: i32,
}

impl WasmiModule {
    pub fn from_valid_module_with_ctx(
        valid_module: Arc<ValidModule>,
        ctx: MappingContext,
        host_metrics: Arc<HostMetrics>,
    ) -> Result<Self, anyhow::Error> {
        let user_module = &valid_module.user_module;
        let mut linker = wasmtime::Linker::new(valid_module.module.store());

        // Used by exports to access the module context. It is `None` while the module is not yet
        // instantiated. A desirable consequence is that start function cannot access host exports.
        let shared_module: Rc<RefCell<Option<WasmiModule>>> = Rc::new(RefCell::new(None));

        macro_rules! link {
            ($wasm_name:expr, $rust_name:ident, $($param:ident: $ty:ty),*) => {
                let func_shared_module = shared_module.clone();
                linker.func(
                    user_module,
                    $wasm_name,
                    move |$($param: $ty),*| {
                        let mut module = func_shared_module.borrow_mut();
                        let module = module.as_mut().unwrap();
                        let _section = module.stopwatch_host_export_other();
                        module.$rust_name(
                            $($param.into()),*
                        ).into_wasm_ret()
                    }
                )?;
            };
        }

        let func_shared_module = shared_module.clone();
        linker.func(
            "env",
            "abort",
            move |message_ptr: i32, file_name_ptr: i32, line_number: i32, column_number: i32| {
                let mut module = func_shared_module.borrow_mut();
                let module = module.as_mut().unwrap();
                module.abort(
                    message_ptr.into(),
                    file_name_ptr.into(),
                    line_number,
                    column_number,
                )
            },
        )?;

        let func_shared_module = shared_module.clone();
        linker.func(
            user_module,
            "store.set",
            move |entity_ptr: i32, id_ptr: i32, data_ptr: i32| {
                let mut module = func_shared_module.borrow_mut();
                let module = module.as_mut().unwrap();
                let stopwatch = &module.host_metrics.stopwatch;
                let _section = stopwatch.start_section("host_export_store_set");
                module.store_set(entity_ptr.into(), id_ptr.into(), data_ptr.into())
            },
        )?;

        let func_shared_module = shared_module.clone();
        linker.func(
            user_module,
            "store.get",
            move |entity_ptr: i32, id_ptr: i32| {
                let start = Instant::now();
                let mut module = func_shared_module.borrow_mut();
                let module = module.as_mut().unwrap();
                let stopwatch = &module.host_metrics.stopwatch;
                let _section = stopwatch.start_section("host_export_store_get");
                let ret = module
                    .store_get(entity_ptr.into(), id_ptr.into())?
                    .wasm_ptr();
                module
                    .host_metrics
                    .observe_host_fn_execution_time(start.elapsed().as_secs_f64(), "store_get");
                Ok(ret)
            },
        )?;

        let func_shared_module = shared_module.clone();
        linker.func(user_module, "ethereum.call", move |call_ptr: i32| {
            let start = Instant::now();
            let mut module = func_shared_module.borrow_mut();
            let module = module.as_mut().unwrap();
            let stopwatch = &module.host_metrics.stopwatch;
            let _section = stopwatch.start_section("host_export_ethereum_call");

            // For apiVersion >= 0.0.4 the call passed from the mapping includes the
            // function signature; subgraphs using an apiVersion < 0.0.4 don't pass
            // the the signature along with the call.
            let arg = if module.ctx.host_exports.api_version >= Version::new(0, 0, 4) {
                module.asc_get::<_, AscUnresolvedContractCall_0_0_4>(call_ptr.into())
            } else {
                module.asc_get::<_, AscUnresolvedContractCall>(call_ptr.into())
            };

            let ret = module.ethereum_call(arg)?.wasm_ptr();
            module
                .host_metrics
                .observe_host_fn_execution_time(start.elapsed().as_secs_f64(), "ethereum_call");
            Ok(ret)
        })?;

        link!("store.remove", store_remove, entity_ptr: i32, id_ptr: i32);

        link!("typeConversion.bytesToString", bytes_to_string, ptr: i32);
        link!("typeConversion.bytesToHex", bytes_to_hex, ptr: i32);
        link!("typeConversion.bigIntToString", big_int_to_string, ptr: i32);
        link!("typeConversion.bigIntToHex", big_int_to_hex, ptr: i32);
        link!("typeConversion.stringToH160", string_to_h160, ptr: i32);

        link!("json.fromBytes", json_from_bytes, ptr: i32);
        link!("json.try_FromBytes", json_try_from_bytes, ptr: i32);
        link!("json.toI64", json_to_i64, ptr: i32);
        link!("json.toU64", json_to_u64, ptr: i32);
        link!("json.toF64", json_to_f64, ptr: i32);
        link!("json.toBigInt", json_to_big_int, ptr: i32);

        link!("ipfs.cat", ipfs_cat, ptr: i32);
        link!(
            "ipfs.map",
            ipfs_map,
            link_ptr: i32,
            callback: i32,
            user_data: i32,
            flags: i32
        );

        link!("crypto.keccak256", crypto_keccak_256, ptr: i32);

        link!("bigInt.plus", big_int_plus, x_ptr: i32, y_ptr: i32);
        link!("bigInt.minus", big_int_minus, x_ptr: i32, y_ptr: i32);
        link!("bigInt.times", big_int_times, x_ptr: i32, y_ptr: i32);
        link!("bigInt.divedBy", big_int_divided_by, x_ptr: i32, y_ptr: i32);

        let module = linker.instantiate(&valid_module.module)?;

        // Provide access to the WASM runtime linear memory
        let memory = module
            .get_memory("memory")
            .context("Failed to find memory export in the WASM module")?;

        let memory_allocate = module
            .get_func("memory.allocate")
            .context("`memory.allocate` function not found")?
            .get1()?;

        let this = WasmiModule {
            module,
            memory_allocate: Box::new(memory_allocate),
            memory,
            ctx,
            valid_module: valid_module.clone(),
            host_metrics,

            // `arena_start_ptr` will be set on the first call to `raw_new`.
            arena_free_size: 0,
            arena_start_ptr: 0,
        };

        Ok(this)
    }

    pub(crate) fn handle_json_callback(
        mut self,
        handler_name: &str,
        value: &serde_json::Value,
        user_data: &store::Value,
    ) -> Result<BlockState, anyhow::Error> {
        let value = self.asc_new(value);
        let user_data = self.asc_new(user_data);

        // Invoke the callback
        self.module
            .get_func(handler_name)
            .with_context(|| format!("function {} not found", handler_name))?
            .get2()?(value.wasm_ptr(), user_data.wasm_ptr())
        .with_context(|| format!("Failed to handle callback '{}'", handler_name))?;

        Ok(self.ctx.state)
    }

    pub(crate) fn handle_ethereum_log(
        mut self,
        handler_name: &str,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        params: Vec<LogParam>,
    ) -> Result<BlockState, anyhow::Error> {
        let block = self.ctx.block.clone();

        // Prepare an EthereumEvent for the WASM runtime
        // Decide on the destination type using the mapping
        // api version provided in the subgraph manifest
        let event = if self.ctx.host_exports.api_version >= Version::new(0, 0, 2) {
            self.asc_new::<AscEthereumEvent<AscEthereumTransaction_0_0_2>, _>(&EthereumEventData {
                block: EthereumBlockData::from(block.as_ref()),
                transaction: EthereumTransactionData::from(transaction.deref()),
                address: log.address,
                log_index: log.log_index.unwrap_or(U256::zero()),
                transaction_log_index: log.log_index.unwrap_or(U256::zero()),
                log_type: log.log_type.clone(),
                params,
            })
            .erase()
        } else {
            self.asc_new::<AscEthereumEvent<AscEthereumTransaction>, _>(&EthereumEventData {
                block: EthereumBlockData::from(block.as_ref()),
                transaction: EthereumTransactionData::from(transaction.deref()),
                address: log.address,
                log_index: log.log_index.unwrap_or(U256::zero()),
                transaction_log_index: log.log_index.unwrap_or(U256::zero()),
                log_type: log.log_type.clone(),
                params,
            })
            .erase()
        };

        // Invoke the event handler
        self.invoke_handler(handler_name, event)?;

        // Return the output state
        Ok(self.ctx.state)
    }

    pub(crate) fn handle_ethereum_call(
        mut self,
        handler_name: &str,
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        inputs: Vec<LogParam>,
        outputs: Vec<LogParam>,
    ) -> Result<BlockState, anyhow::Error> {
        let call = EthereumCallData {
            to: call.to,
            from: call.from,
            block: EthereumBlockData::from(self.ctx.block.as_ref()),
            transaction: EthereumTransactionData::from(transaction.deref()),
            inputs,
            outputs,
        };
        let arg = if self.ctx.host_exports.api_version >= Version::new(0, 0, 3) {
            self.asc_new::<AscEthereumCall_0_0_3, _>(&call).erase()
        } else {
            self.asc_new::<AscEthereumCall, _>(&call).erase()
        };

        self.invoke_handler(handler_name, arg)?;

        Ok(self.ctx.state)
    }

    pub(crate) fn handle_ethereum_block(
        mut self,
        handler_name: &str,
    ) -> Result<BlockState, anyhow::Error> {
        // Prepare an EthereumBlock for the WASM runtime
        let arg = self.asc_new(&EthereumBlockData::from(self.ctx.block.as_ref()));

        self.invoke_handler(handler_name, arg)?;

        Ok(self.ctx.state)
    }

    fn invoke_handler<C>(&self, handler: &str, arg: AscPtr<C>) -> Result<(), anyhow::Error> {
        self.module
            .get_func(handler)
            .with_context(|| format!("function {} not found", handler))?
            .get1()?(arg.wasm_ptr())
        .with_context(|| format!("Failed to invoke handler '{}'", handler))
    }

    fn stopwatch_host_export_other(&self) -> graph::components::metrics::stopwatch::Section {
        self.host_metrics
            .stopwatch
            .start_section("host_export_other")
    }
}

impl AscHeap for WasmiModule {
    fn raw_new(&mut self, bytes: &[u8]) -> u32 {
        // We request large chunks from the AssemblyScript allocator to use as arenas that we
        // manage directly.

        static MIN_ARENA_SIZE: i32 = 10_000;

        let size = i32::try_from(bytes.len()).unwrap();
        if size > self.arena_free_size {
            // Allocate a new arena. Any free space left in the previous arena is left unused. This
            // causes at most half of memory to be wasted, which is acceptable.
            let arena_size = size.max(MIN_ARENA_SIZE);
            self.arena_start_ptr = (self.memory_allocate)(arena_size).unwrap();
            self.arena_free_size = arena_size;
        };

        let ptr = self.arena_start_ptr as usize;

        // Safe because we are accessing and immediately dropping the reference to the data.
        unsafe { self.memory.data_unchecked_mut()[ptr..bytes.len()].copy_from_slice(bytes) }
        self.arena_start_ptr += size;
        self.arena_free_size -= size;

        ptr as u32
    }

    fn get(&self, offset: u32, size: u32) -> Vec<u8> {
        let offset = offset as usize;
        let size = size as usize;

        // Safe because we are accessing and immediately dropping the reference to the data.
        unsafe { self.memory.data_unchecked()[offset..(offset + size)].to_vec() }
    }
}

// Implementation of externals.
impl WasmiModule {
    /// function abort(message?: string | null, fileName?: string | null, lineNumber?: u32, columnNumber?: u32): void
    /// Always returns a trap.
    fn abort(
        &self,
        message_ptr: AscPtr<AscString>,
        file_name_ptr: AscPtr<AscString>,
        line_number: i32,
        column_number: i32,
    ) -> Result<(), Trap> {
        let message = match message_ptr.is_null() {
            false => Some(self.asc_get(message_ptr)),
            true => None,
        };
        let file_name = match file_name_ptr.is_null() {
            false => Some(self.asc_get(file_name_ptr)),
            true => None,
        };
        let line_number = match line_number {
            0 => None,
            _ => Some(line_number),
        };
        let column_number = match column_number {
            0 => None,
            _ => Some(column_number),
        };
        Err(self
            .ctx
            .host_exports
            .abort(message, file_name, line_number, column_number)
            .unwrap_err()
            .into())
    }

    /// function store.set(entity: string, id: string, data: Entity): void
    fn store_set(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscEntity>,
    ) -> Result<(), Trap> {
        let entity = self.asc_get(entity_ptr);
        let id = self.asc_get(id_ptr);
        let data = self.try_asc_get(data_ptr)?;
        self.ctx.host_exports.store_set(
            &self.ctx.logger,
            &mut self.ctx.state,
            &self.ctx.proof_of_indexing,
            entity,
            id,
            data,
        )?;
        Ok(())
    }

    /// function store.remove(entity: string, id: string): void
    fn store_remove(&mut self, entity_ptr: AscPtr<AscString>, id_ptr: AscPtr<AscString>) {
        let entity = self.asc_get(entity_ptr);
        let id = self.asc_get(id_ptr);
        self.ctx.host_exports.store_remove(
            &self.ctx.logger,
            &mut self.ctx.state,
            &self.ctx.proof_of_indexing,
            entity,
            id,
        );
    }

    /// function store.get(entity: string, id: string): Entity | null
    fn store_get(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscEntity>, Trap> {
        let entity_ptr = self.asc_get(entity_ptr);
        let id_ptr = self.asc_get(id_ptr);
        let entity_option =
            self.ctx
                .host_exports
                .store_get(&mut self.ctx.state, entity_ptr, id_ptr)?;

        Ok(match entity_option {
            Some(entity) => {
                let _section = self
                    .host_metrics
                    .stopwatch
                    .start_section("store_get_asc_new");
                self.asc_new(&entity)
            }
            None => AscPtr::null(),
        })
    }

    /// function ethereum.call(call: SmartContractCall): Array<Token> | null
    fn ethereum_call(
        &mut self,
        call: UnresolvedContractCall,
    ) -> Result<AscEnumArray<EthereumValueKind>, Trap> {
        let result =
            self.ctx
                .host_exports
                .ethereum_call(&self.ctx.logger, &self.ctx.block, call)?;
        Ok(match result {
            Some(tokens) => self.asc_new(tokens.as_slice()),
            None => AscPtr::null(),
        })
    }

    /// function typeConversion.bytesToString(bytes: Bytes): string
    fn bytes_to_string(&mut self, bytes_ptr: AscPtr<Uint8Array>) -> AscPtr<AscString> {
        let string = host_exports::bytes_to_string(&self.ctx.logger, self.asc_get(bytes_ptr));
        self.asc_new(&string)
    }

    /// Converts bytes to a hex string.
    /// function typeConversion.bytesToHex(bytes: Bytes): string
    /// References:
    /// https://godoc.org/github.com/ethereum/go-ethereum/common/hexutil#hdr-Encoding_Rules
    /// https://github.com/ethereum/web3.js/blob/f98fe1462625a6c865125fecc9cb6b414f0a5e83/packages/web3-utils/src/utils.js#L283
    fn bytes_to_hex(&mut self, bytes_ptr: AscPtr<Uint8Array>) -> AscPtr<AscString> {
        let bytes: Vec<u8> = self.asc_get(bytes_ptr);
        // Even an empty string must be prefixed with `0x`.
        // Encodes each byte as a two hex digits.
        let hex = format!("0x{}", hex::encode(bytes));
        self.asc_new(&hex)
    }

    /// function typeConversion.bigIntToString(n: Uint8Array): string
    fn big_int_to_string(&mut self, big_int_ptr: AscPtr<AscBigInt>) -> AscPtr<AscString> {
        let n: BigInt = self.asc_get(big_int_ptr);
        self.asc_new(&n.to_string())
    }

    /// function typeConversion.bigIntToHex(n: Uint8Array): string
    fn big_int_to_hex(&mut self, big_int_ptr: AscPtr<AscBigInt>) -> AscPtr<AscString> {
        let n: BigInt = self.asc_get(big_int_ptr);
        let hex = self.ctx.host_exports.big_int_to_hex(n);
        self.asc_new(&hex)
    }

    /// function typeConversion.stringToH160(s: String): H160
    fn string_to_h160(&mut self, str_ptr: AscPtr<AscString>) -> Result<AscPtr<AscH160>, Trap> {
        let s: String = self.asc_get(str_ptr);
        let h160 = host_exports::string_to_h160(&s)?;
        let h160_obj: AscPtr<AscH160> = self.asc_new(&h160);
        Ok(h160_obj)
    }

    /// function json.fromBytes(bytes: Bytes): JSONValue
    fn json_from_bytes(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscEnum<JsonValueKind>>, Trap> {
        let bytes: Vec<u8> = self.asc_get(bytes_ptr);

        let result = host_exports::json_from_bytes(&bytes).with_context(|| {
            format!("Failed to parse JSON from byte array. Bytes: `{:?}`", bytes,)
        })?;
        Ok(self.asc_new(&result))
    }

    /// function json.try_fromBytes(bytes: Bytes): Result<JSONValue, boolean>
    fn json_try_from_bytes(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscResult<AscEnum<JsonValueKind>, bool>>, Trap> {
        let bytes: Vec<u8> = self.asc_get(bytes_ptr);
        let result = host_exports::json_from_bytes(&bytes).map_err(|e| {
            warn!(
                &self.ctx.logger,
                "Failed to parse JSON from byte array";
                "bytes" => format!("{:?}", bytes),
                "error" => format!("{}", e)
            );

            // Map JSON errors to boolean to match the `Result<JSONValue, boolean>`
            // result type expected by mappings
            true
        });
        Ok(self.asc_new(&result))
    }

    /// function ipfs.cat(link: String): Bytes
    fn ipfs_cat(&mut self, link_ptr: AscPtr<AscString>) -> Result<AscPtr<Uint8Array>, Trap> {
        let link = self.asc_get(link_ptr);
        let ipfs_res = self.ctx.host_exports.ipfs_cat(&self.ctx.logger, link);
        match ipfs_res {
            Ok(bytes) => {
                let bytes_obj: AscPtr<Uint8Array> = self.asc_new(&*bytes);
                Ok(bytes_obj)
            }

            // Return null in case of error.
            Err(e) => {
                info!(&self.ctx.logger, "Failed ipfs.cat, returning `null`";
                                    "link" => self.asc_get::<String, _>(link_ptr),
                                    "error" => e.to_string());
                Ok(AscPtr::null())
            }
        }
    }

    /// function ipfs.map(link: String, callback: String, flags: String[]): void
    fn ipfs_map(
        &mut self,
        link_ptr: AscPtr<AscString>,
        callback: AscPtr<AscString>,
        user_data: AscPtr<AscEnum<StoreValueKind>>,
        flags: AscPtr<Array<AscPtr<AscString>>>,
    ) -> Result<(), Trap> {
        let link: String = self.asc_get(link_ptr);
        let callback: String = self.asc_get(callback);
        let user_data: store::Value = self.try_asc_get(user_data)?;

        let flags = self.asc_get(flags);
        let start_time = Instant::now();
        let output_states = HostExports::ipfs_map(
            &self.ctx.host_exports.link_resolver.clone(),
            self,
            link.clone(),
            &*callback,
            user_data,
            flags,
        )?;

        debug!(
            &self.ctx.logger,
            "Successfully processed file with ipfs.map";
            "link" => &link,
            "callback" => &*callback,
            "n_calls" => output_states.len(),
            "time" => format!("{}ms", start_time.elapsed().as_millis())
        );
        for output_state in output_states {
            self.ctx
                .state
                .entity_cache
                .extend(output_state.entity_cache)
                .map_err(anyhow::Error::from)?;
            self.ctx
                .state
                .created_data_sources
                .extend(output_state.created_data_sources);
        }

        // Advance this module's start time by the time it took to run the entire
        // ipfs_map. This has the effect of not charging this module for the time
        // spent running the callback on every JSON object in the IPFS file

        // TODO
        // self.start_time += start_time.elapsed();
        Ok(())
    }

    /// Expects a decimal string.
    /// function json.toI64(json: String): i64
    fn json_to_i64(&mut self, json_ptr: AscPtr<AscString>) -> Result<i64, Trap> {
        let number = self.ctx.host_exports.json_to_i64(self.asc_get(json_ptr))?;
        Ok(number)
    }

    /// Expects a decimal string.
    /// function json.toU64(json: String): u64
    fn json_to_u64(&mut self, json_ptr: AscPtr<AscString>) -> Result<u64, Trap> {
        Ok(self.ctx.host_exports.json_to_u64(self.asc_get(json_ptr))?)
    }

    /// Expects a decimal string.
    /// function json.toF64(json: String): f64
    fn json_to_f64(&mut self, json_ptr: AscPtr<AscString>) -> Result<f64, Trap> {
        Ok(self.ctx.host_exports.json_to_f64(self.asc_get(json_ptr))?)
    }

    /// Expects a decimal string.
    /// function json.toBigInt(json: String): BigInt
    fn json_to_big_int(&mut self, json_ptr: AscPtr<AscString>) -> Result<AscPtr<AscBigInt>, Trap> {
        let big_int = self
            .ctx
            .host_exports
            .json_to_big_int(self.asc_get(json_ptr))?;
        let big_int_ptr: AscPtr<AscBigInt> = self.asc_new(&*big_int);
        Ok(big_int_ptr)
    }

    /// function crypto.keccak256(input: Bytes): Bytes
    fn crypto_keccak_256(
        &mut self,
        input_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<Uint8Array>, Trap> {
        let input = self
            .ctx
            .host_exports
            .crypto_keccak_256(self.asc_get(input_ptr));
        let hash_ptr: AscPtr<Uint8Array> = self.asc_new(input.as_ref());
        Ok(hash_ptr)
    }

    /// function bigInt.plus(x: BigInt, y: BigInt): BigInt
    fn big_int_plus(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_int_plus(self.asc_get(x_ptr), self.asc_get(y_ptr));
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(result_ptr)
    }

    /// function bigInt.minus(x: BigInt, y: BigInt): BigInt
    fn big_int_minus(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_int_minus(self.asc_get(x_ptr), self.asc_get(y_ptr));
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(result_ptr)
    }

    /// function bigInt.times(x: BigInt, y: BigInt): BigInt
    fn big_int_times(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_int_times(self.asc_get(x_ptr), self.asc_get(y_ptr));
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(result_ptr)
    }

    /// function bigInt.dividedBy(x: BigInt, y: BigInt): BigInt
    fn big_int_divided_by(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_int_divided_by(self.asc_get(x_ptr), self.asc_get(y_ptr))?;
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(result_ptr)
    }

    /// function bigInt.dividedByDecimal(x: BigInt, y: BigDecimal): BigDecimal
    fn big_int_divided_by_decimal(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, Trap> {
        let x = BigDecimal::new(self.asc_get::<BigInt, _>(x_ptr), 0);
        let result = self
            .ctx
            .host_exports
            .big_decimal_divided_by(x, self.try_asc_get(y_ptr)?)?;
        Ok(self.asc_new(&result))
    }

    /// function bigInt.mod(x: BigInt, y: BigInt): BigInt
    fn big_int_mod(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_int_mod(self.asc_get(x_ptr), self.asc_get(y_ptr));
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(result_ptr)
    }

    /// function bigInt.pow(x: BigInt, exp: u8): BigInt
    fn big_int_pow(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        exp: u8,
    ) -> Result<AscPtr<AscBigInt>, Trap> {
        let result = self.ctx.host_exports.big_int_pow(self.asc_get(x_ptr), exp);
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(result_ptr)
    }

    /// function typeConversion.bytesToBase58(bytes: Bytes): string
    fn bytes_to_base58(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscString>, Trap> {
        let result = self
            .ctx
            .host_exports
            .bytes_to_base58(self.asc_get(bytes_ptr));
        let result_ptr: AscPtr<AscString> = self.asc_new(&result);
        Ok(result_ptr)
    }

    /// function bigDecimal.toString(x: BigDecimal): string
    fn big_decimal_to_string(
        &mut self,
        big_decimal_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscString>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_to_string(self.try_asc_get(big_decimal_ptr)?);
        Ok(self.asc_new(&result))
    }

    /// function bigDecimal.fromString(x: string): BigDecimal
    fn big_decimal_from_string(
        &mut self,
        string_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscBigDecimal>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_from_string(self.asc_get(string_ptr))?;
        Ok(self.asc_new(&result))
    }

    /// function bigDecimal.plus(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_plus(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_plus(self.try_asc_get(x_ptr)?, self.try_asc_get(y_ptr)?);
        Ok(self.asc_new(&result))
    }

    /// function bigDecimal.minus(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_minus(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_minus(self.try_asc_get(x_ptr)?, self.try_asc_get(y_ptr)?);
        Ok(self.asc_new(&result))
    }

    /// function bigDecimal.times(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_times(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_times(self.try_asc_get(x_ptr)?, self.try_asc_get(y_ptr)?);
        Ok(self.asc_new(&result))
    }

    /// function bigDecimal.dividedBy(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_divided_by(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_divided_by(self.try_asc_get(x_ptr)?, self.try_asc_get(y_ptr)?)?;
        Ok(self.asc_new(&result))
    }

    /// function bigDecimal.equals(x: BigDecimal, y: BigDecimal): bool
    fn big_decimal_equals(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<bool, Trap> {
        Ok(self
            .ctx
            .host_exports
            .big_decimal_equals(self.try_asc_get(x_ptr)?, self.try_asc_get(y_ptr)?))
    }

    /// function dataSource.create(name: string, params: Array<string>): void
    fn data_source_create(
        &mut self,
        name_ptr: AscPtr<AscString>,
        params_ptr: AscPtr<Array<AscPtr<AscString>>>,
    ) -> Result<(), Trap> {
        let name: String = self.asc_get(name_ptr);
        let params: Vec<String> = self.asc_get(params_ptr);
        self.ctx.host_exports.data_source_create(
            &self.ctx.logger,
            &mut self.ctx.state,
            name,
            params,
            None,
        )?;
        Ok(())
    }

    /// function createWithContext(name: string, params: Array<string>, context: DataSourceContext): void
    fn data_source_create_with_context(
        &mut self,
        name_ptr: AscPtr<AscString>,
        params_ptr: AscPtr<Array<AscPtr<AscString>>>,
        context_ptr: AscPtr<AscEntity>,
    ) -> Result<(), Trap> {
        let name: String = self.asc_get(name_ptr);
        let params: Vec<String> = self.asc_get(params_ptr);
        let context: HashMap<_, _> = self.try_asc_get(context_ptr)?;
        self.ctx.host_exports.data_source_create(
            &self.ctx.logger,
            &mut self.ctx.state,
            name,
            params,
            Some(context.into()),
        )?;
        Ok(())
    }

    /// function dataSource.address(): Bytes
    fn data_source_address(&mut self) -> AscPtr<Uint8Array> {
        self.asc_new(&self.ctx.host_exports.data_source_address())
    }

    /// function dataSource.network(): String
    fn data_source_network(&mut self) -> AscPtr<AscString> {
        self.asc_new(&self.ctx.host_exports.data_source_network())
    }

    /// function dataSource.context(): DataSourceContext
    fn data_source_context(&mut self) -> AscPtr<AscEntity> {
        self.asc_new(&self.ctx.host_exports.data_source_context())
    }

    fn ens_name_by_hash(&mut self, hash_ptr: AscPtr<AscString>) -> Result<AscPtr<AscString>, Trap> {
        let hash: String = self.asc_get(hash_ptr);
        let name = self.ctx.host_exports.ens_name_by_hash(&*hash)?;
        // map `None` to `null`, and `Some(s)` to a runtime string
        Ok(name
            .map(|name| self.asc_new(&*name))
            .unwrap_or(AscPtr::null()))
    }

    fn log_log(&mut self, level: i32, msg: AscPtr<AscString>) {
        let level = LogLevel::from(level).into();
        let msg: String = self.asc_get(msg);
        self.ctx.host_exports.log_log(&self.ctx.logger, level, msg);
    }

    /// function arweave.transactionData(txId: string): Bytes | null
    fn arweave_transaction_data(
        &mut self,
        tx_id: AscPtr<AscString>,
    ) -> Result<AscPtr<Uint8Array>, Trap> {
        let tx_id: String = self.asc_get(tx_id);
        let data = self.ctx.host_exports.arweave_transaction_data(&tx_id);
        Ok(data
            .map(|data| self.asc_new(&*data))
            .unwrap_or(AscPtr::null()))
    }

    /// function box.profile(address: string): JSONValue | null
    fn box_profile(&mut self, address: AscPtr<AscString>) -> Result<AscPtr<AscJson>, Trap> {
        let address: String = self.asc_get(address);
        let profile = self.ctx.host_exports.box_profile(&address);
        Ok(profile
            .map(|profile| self.asc_new(&profile))
            .unwrap_or(AscPtr::null()))
    }
}

#[cfg(any())]
impl Externals for WasmiModule {
    fn invoke_index(&mut self, index: usize, args: RuntimeArgs) -> Result<AscPtr<()>, Trap> {
        // Start a catch-all section for exports that don't have their own section.
        let stopwatch = self.host_metrics.stopwatch.clone();
        let _section = stopwatch.start_section("host_export_other");
        let start = Instant::now();
        let res = match index {
            STORE_SET_FUNC_INDEX => {
                let _section = stopwatch.start_section("host_export_store_set");
                self.store_set(
                    args.nth_checked(0)?,
                    args.nth_checked(1)?,
                    args.nth_checked(2)?,
                )
            }
            STORE_GET_FUNC_INDEX => {
                let _section = stopwatch.start_section("host_export_store_get");
                self.store_get(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            STORE_REMOVE_FUNC_INDEX => {
                self.store_remove(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            ETHEREUM_CALL_FUNC_INDEX => {
                let _section = stopwatch.start_section("host_export_ethereum_call");

                // For apiVersion >= 0.0.4 the call passed from the mapping includes the
                // function signature; subgraphs using an apiVersion < 0.0.4 don't pass
                // the the signature along with the call.
                let arg = if self.ctx.host_exports.api_version >= Version::new(0, 0, 4) {
                    self.asc_get::<_, AscUnresolvedContractCall_0_0_4>(args.nth_checked(0)?)
                } else {
                    self.asc_get::<_, AscUnresolvedContractCall>(args.nth_checked(0)?)
                };

                self.ethereum_call(arg)
            }
            TYPE_CONVERSION_BYTES_TO_STRING_FUNC_INDEX => {
                self.bytes_to_string(args.nth_checked(0)?)
            }
            TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX => self.bytes_to_hex(args.nth_checked(0)?),
            TYPE_CONVERSION_BIG_INT_TO_STRING_FUNC_INDEX => {
                self.big_int_to_string(args.nth_checked(0)?)
            }
            TYPE_CONVERSION_BIG_INT_TO_HEX_FUNC_INDEX => self.big_int_to_hex(args.nth_checked(0)?),
            TYPE_CONVERSION_STRING_TO_H160_FUNC_INDEX => self.string_to_h160(args.nth_checked(0)?),
            JSON_FROM_BYTES_FUNC_INDEX => self.json_from_bytes(args.nth_checked(0)?),
            JSON_TO_I64_FUNC_INDEX => self.json_to_i64(args.nth_checked(0)?),
            JSON_TO_U64_FUNC_INDEX => self.json_to_u64(args.nth_checked(0)?),
            JSON_TO_F64_FUNC_INDEX => self.json_to_f64(args.nth_checked(0)?),
            JSON_TO_BIG_INT_FUNC_INDEX => self.json_to_big_int(args.nth_checked(0)?),
            IPFS_CAT_FUNC_INDEX => {
                let _section = stopwatch.start_section("host_export_ipfs_cat");
                self.ipfs_cat(args.nth_checked(0)?)
            }
            CRYPTO_KECCAK_256_INDEX => self.crypto_keccak_256(args.nth_checked(0)?),
            BIG_INT_PLUS => self.big_int_plus(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_INT_MINUS => self.big_int_minus(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_INT_TIMES => self.big_int_times(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_INT_DIVIDED_BY => {
                self.big_int_divided_by(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            BIG_INT_DIVIDED_BY_DECIMAL => {
                self.big_int_divided_by_decimal(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            BIG_INT_MOD => self.big_int_mod(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_INT_POW => self.big_int_pow(args.nth_checked(0)?, args.nth_checked(1)?),
            TYPE_CONVERSION_BYTES_TO_BASE_58_INDEX => self.bytes_to_base58(args.nth_checked(0)?),
            BIG_DECIMAL_PLUS => self.big_decimal_plus(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_DECIMAL_MINUS => self.big_decimal_minus(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_DECIMAL_TIMES => self.big_decimal_times(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_DECIMAL_DIVIDED_BY => {
                self.big_decimal_divided_by(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            BIG_DECIMAL_EQUALS => {
                self.big_decimal_equals(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            BIG_DECIMAL_TO_STRING => self.big_decimal_to_string(args.nth_checked(0)?),
            BIG_DECIMAL_FROM_STRING => self.big_decimal_from_string(args.nth_checked(0)?),
            IPFS_MAP_FUNC_INDEX => {
                let _section = stopwatch.start_section("host_export_ipfs_map");
                self.ipfs_map(
                    args.nth_checked(0)?,
                    args.nth_checked(1)?,
                    args.nth_checked(2)?,
                    args.nth_checked(3)?,
                )
            }
            DATA_SOURCE_CREATE_INDEX => {
                self.data_source_create(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            ENS_NAME_BY_HASH => self.ens_name_by_hash(args.nth_checked(0)?),
            LOG_LOG => self.log_log(args.nth_checked(0)?, args.nth_checked(1)?),
            DATA_SOURCE_ADDRESS => self.data_source_address(),
            DATA_SOURCE_NETWORK => self.data_source_network(),
            DATA_SOURCE_CREATE_WITH_CONTEXT => self.data_source_create_with_context(
                args.nth_checked(0)?,
                args.nth_checked(1)?,
                args.nth_checked(2)?,
            ),
            DATA_SOURCE_CONTEXT => self.data_source_context(),
            JSON_TRY_FROM_BYTES_FUNC_INDEX => self.json_try_from_bytes(args.nth_checked(0)?),
            ARWEAVE_TRANSACTION_DATA => self.arweave_transaction_data(args.nth_checked(0)?),
            BOX_PROFILE => self.box_profile(args.nth_checked(0)?),
            _ => panic!("Unimplemented function at {}", index),
        };

        res
    }
}

pub struct ModuleResolver;

#[cfg(any())]
impl ModuleImportResolver for ModuleResolver {
    fn resolve_func(&self, field_name: &str, signature: &Signature) -> Result<FuncRef, Error> {
        let signature = signature.clone();
        Ok(match field_name {
            // store
            "store.set" => FuncInstance::alloc_host(signature, STORE_SET_FUNC_INDEX),
            "store.remove" => FuncInstance::alloc_host(signature, STORE_REMOVE_FUNC_INDEX),
            "store.get" => FuncInstance::alloc_host(signature, STORE_GET_FUNC_INDEX),

            // ethereum
            "ethereum.call" => FuncInstance::alloc_host(signature, ETHEREUM_CALL_FUNC_INDEX),

            // typeConversion
            "typeConversion.bytesToString" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BYTES_TO_STRING_FUNC_INDEX)
            }
            "typeConversion.bytesToHex" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX)
            }
            "typeConversion.bigIntToString" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BIG_INT_TO_STRING_FUNC_INDEX)
            }
            "typeConversion.bigIntToHex" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BIG_INT_TO_HEX_FUNC_INDEX)
            }
            "typeConversion.stringToH160" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_STRING_TO_H160_FUNC_INDEX)
            }
            "typeConversion.bytesToBase58" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BYTES_TO_BASE_58_INDEX)
            }

            // json
            "json.fromBytes" => FuncInstance::alloc_host(signature, JSON_FROM_BYTES_FUNC_INDEX),
            "json.try_fromBytes" => {
                FuncInstance::alloc_host(signature, JSON_TRY_FROM_BYTES_FUNC_INDEX)
            }
            "json.toI64" => FuncInstance::alloc_host(signature, JSON_TO_I64_FUNC_INDEX),
            "json.toU64" => FuncInstance::alloc_host(signature, JSON_TO_U64_FUNC_INDEX),
            "json.toF64" => FuncInstance::alloc_host(signature, JSON_TO_F64_FUNC_INDEX),
            "json.toBigInt" => FuncInstance::alloc_host(signature, JSON_TO_BIG_INT_FUNC_INDEX),

            // ipfs
            "ipfs.cat" => FuncInstance::alloc_host(signature, IPFS_CAT_FUNC_INDEX),
            "ipfs.map" => FuncInstance::alloc_host(signature, IPFS_MAP_FUNC_INDEX),

            // crypto
            "crypto.keccak256" => FuncInstance::alloc_host(signature, CRYPTO_KECCAK_256_INDEX),

            // bigInt
            "bigInt.plus" => FuncInstance::alloc_host(signature, BIG_INT_PLUS),
            "bigInt.minus" => FuncInstance::alloc_host(signature, BIG_INT_MINUS),
            "bigInt.times" => FuncInstance::alloc_host(signature, BIG_INT_TIMES),
            "bigInt.dividedBy" => FuncInstance::alloc_host(signature, BIG_INT_DIVIDED_BY),
            "bigInt.dividedByDecimal" => {
                FuncInstance::alloc_host(signature, BIG_INT_DIVIDED_BY_DECIMAL)
            }
            "bigInt.mod" => FuncInstance::alloc_host(signature, BIG_INT_MOD),
            "bigInt.pow" => FuncInstance::alloc_host(signature, BIG_INT_POW),

            // bigDecimal
            "bigDecimal.plus" => FuncInstance::alloc_host(signature, BIG_DECIMAL_PLUS),
            "bigDecimal.minus" => FuncInstance::alloc_host(signature, BIG_DECIMAL_MINUS),
            "bigDecimal.times" => FuncInstance::alloc_host(signature, BIG_DECIMAL_TIMES),
            "bigDecimal.dividedBy" => FuncInstance::alloc_host(signature, BIG_DECIMAL_DIVIDED_BY),
            "bigDecimal.equals" => FuncInstance::alloc_host(signature, BIG_DECIMAL_EQUALS),
            "bigDecimal.toString" => FuncInstance::alloc_host(signature, BIG_DECIMAL_TO_STRING),
            "bigDecimal.fromString" => FuncInstance::alloc_host(signature, BIG_DECIMAL_FROM_STRING),

            // dataSource
            "dataSource.create" => FuncInstance::alloc_host(signature, DATA_SOURCE_CREATE_INDEX),
            "dataSource.address" => FuncInstance::alloc_host(signature, DATA_SOURCE_ADDRESS),
            "dataSource.network" => FuncInstance::alloc_host(signature, DATA_SOURCE_NETWORK),
            "dataSource.createWithContext" => {
                FuncInstance::alloc_host(signature, DATA_SOURCE_CREATE_WITH_CONTEXT)
            }
            "dataSource.context" => FuncInstance::alloc_host(signature, DATA_SOURCE_CONTEXT),

            // ens.nameByHash
            "ens.nameByHash" => FuncInstance::alloc_host(signature, ENS_NAME_BY_HASH),

            // log.log
            "log.log" => FuncInstance::alloc_host(signature, LOG_LOG),

            "arweave.transactionData" => {
                FuncInstance::alloc_host(signature, ARWEAVE_TRANSACTION_DATA)
            }
            "box.profile" => FuncInstance::alloc_host(signature, BOX_PROFILE),

            // Unknown export
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )));
            }
        })
    }
}
