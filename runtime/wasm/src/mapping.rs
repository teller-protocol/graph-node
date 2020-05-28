use crate::module::WasmInstance;
use ethabi::LogParam;
use futures::sync::mpsc;
use futures03::channel::oneshot::Sender;
use graph::components::ethereum::*;
use graph::components::subgraph::SharedProofOfIndexing;
use graph::prelude::*;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use strum_macros::AsStaticStr;
use web3::types::{Log, Transaction};

/// Spawn a wasm module in its own thread.
pub fn spawn_module(
    raw_module: Vec<u8>,
    logger: Logger,
    subgraph_id: SubgraphDeploymentId,
    host_metrics: Arc<HostMetrics>,
    runtime: tokio::runtime::Handle,
) -> Result<mpsc::Sender<MappingRequest>, anyhow::Error> {
    // Create channel for event handling requests
    let (mapping_request_sender, mapping_request_receiver) = mpsc::channel(100);
    // wasmtime modules are not `Send` therefore they cannot be scheduled by
    // the regular tokio executor, so we create a dedicated thread.
    //
    // In case of failure, this thread may panic or simply terminate,
    // dropping the `mapping_request_receiver` which ultimately causes the
    // subgraph to fail the next time it tries to handle an event.
    let conf =
        thread::Builder::new().name(format!("mapping-{}-{}", &subgraph_id, uuid::Uuid::new_v4()));
    conf.spawn(move || {
        let valid_module = Arc::new(ValidModule::new(&raw_module).unwrap());
        runtime.enter(|| {
            // Pass incoming triggers to the WASM module and return entity changes;
            // Stop when canceled because all RuntimeHosts and their senders were dropped.
            match mapping_request_receiver
                .map_err(|()| unreachable!())
                .for_each(move |request| {
                    let MappingRequest {
                        ctx,
                        trigger,
                        result_sender,
                    } = request;

                    // Start the WASM module runtime.
                    let section = host_metrics.stopwatch.start_section("module_init");
                    let module = WasmInstance::from_valid_module_with_ctx(
                        valid_module.clone(),
                        ctx,
                        host_metrics.clone(),
                    )?;
                    section.end();

                    let section = host_metrics.stopwatch.start_section("run_handler");
                    let result = match trigger {
                        MappingTrigger::Log {
                            transaction,
                            log,
                            params,
                            handler,
                        } => module.handle_ethereum_log(
                            handler.handler.as_str(),
                            transaction,
                            log,
                            params,
                        ),
                        MappingTrigger::Call {
                            transaction,
                            call,
                            inputs,
                            outputs,
                            handler,
                        } => module.handle_ethereum_call(
                            handler.handler.as_str(),
                            transaction,
                            call,
                            inputs,
                            outputs,
                        ),
                        MappingTrigger::Block { handler } => {
                            module.handle_ethereum_block(handler.handler.as_str())
                        }
                    };
                    section.end();

                    result_sender
                        .send((result, future::ok(Instant::now())))
                        .map_err(|_| anyhow::anyhow!("WASM module result receiver dropped."))
                })
                .wait()
            {
                Ok(()) => debug!(logger, "Subgraph stopped, WASM runtime thread terminated"),
                Err(e) => debug!(logger, "WASM runtime thread terminated abnormally";
                                    "error" => e.to_string()),
            }
        })
    })
    .map(|_| ())
    .context("Spawning WASM runtime thread failed")?;

    Ok(mapping_request_sender)
}

#[derive(Debug, AsStaticStr)]
pub(crate) enum MappingTrigger {
    Log {
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        params: Vec<LogParam>,
        handler: MappingEventHandler,
    },
    Call {
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        inputs: Vec<LogParam>,
        outputs: Vec<LogParam>,
        handler: MappingCallHandler,
    },
    Block {
        handler: MappingBlockHandler,
    },
}

type MappingResponse = (
    Result<BlockState, anyhow::Error>,
    futures::Finished<Instant, Error>,
);

#[derive(Debug)]
pub struct MappingRequest {
    pub(crate) ctx: MappingContext,
    pub(crate) trigger: MappingTrigger,
    pub(crate) result_sender: Sender<MappingResponse>,
}

#[derive(Debug)]
pub(crate) struct MappingContext {
    pub(crate) logger: Logger,
    pub(crate) host_exports: Arc<crate::host_exports::HostExports>,
    pub(crate) block: Arc<LightEthereumBlock>,
    pub(crate) state: BlockState,
    pub(crate) proof_of_indexing: SharedProofOfIndexing,
}

impl MappingContext {
    pub fn derive_with_empty_block_state(&self) -> Self {
        MappingContext {
            logger: self.logger.clone(),
            host_exports: self.host_exports.clone(),
            block: self.block.clone(),
            state: BlockState::new(self.state.entity_cache.store.clone(), Default::default()),
            proof_of_indexing: self.proof_of_indexing.cheap_clone(),
        }
    }
}

/// A pre-processed and valid WASM module, ready to be started as a WasmModule.
pub(crate) struct ValidModule {
    pub(super) module: wasmtime::Module,
    pub(super) user_module: String,
}

impl ValidModule {
    /// Pre-process and validate the module.
    pub fn new(raw_module: &[u8]) -> Result<Self, anyhow::Error> {
        // TODO: Use Store interrupts to check timeouts
        let store = wasmtime::Store::default();
        let module = wasmtime::Module::from_binary(&store, raw_module)?;
        // Collect the names of all modules from which `module` imports something.

        // Hack: AS currently puts all user imports in one module, in addition to the built-in "env"
        // module. The name of that module is not fixed, to able able to infer the name we allow
        // only one module with imports, with "env" (for AS "env.abort") being an exception.
        let mut imported_modules: Vec<_> = module
            .imports()
            .map(|import| import.module())
            .filter(|module| *module != "env")
            .collect();
        imported_modules.dedup();
        let user_module = match imported_modules.len() {
            1 => imported_modules[0].to_owned(),
            _ => anyhow::bail!(
                "WASM module has {} import sections, we require exactly 1",
                imported_modules.len(),
            ),
        };

        Ok(ValidModule {
            module,
            user_module,
        })
    }
}
