import "allocator/arena";

export { memory };

// Graph Node requires some import to exist.
declare namespace dataSources {
  function network(): string;
}

export function abort(): void {
  assert(false, "not true");
  dataSources.network();
}
