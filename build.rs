fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(any(feature = "client-grpc", feature = "server-grpc"))]
    {
        if let Ok(protoc_path) = protoc_bin_vendored::protoc_bin_path() {
            unsafe {
                std::env::set_var("PROTOC", protoc_path);
            }
        }
        tonic_build::configure().compile_protos(&["proto/flotilla.proto"], &["proto"])?;
    }
    Ok(())
}
