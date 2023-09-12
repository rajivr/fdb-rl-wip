# Java RecordLayer Protobufs

The protos present here is imported from Java RecordLayer version [3.3.410.0](https://github.com/FoundationDB/fdb-record-layer/tree/3.3.410.0).

```
fdb-record-layer-core/src/test
|-- proto
|-- proto2
`-- proto3
```

We are preserving the above directory structure. _However_ all files have been adapted to `proto3`, since we do not support `proto2`. We also do not use protobuf file option, message option and field option, so that has also been adjusted.

The `package` name has also been adjusted. In the package name `proto`, `proto2`, `proto3` and `v1` has no significance. We need it to keep `buf` tool happy.


