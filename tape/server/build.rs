fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../proto/tape.proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../proto/tape.proto"], &["../proto"])?;
    Ok(())
}
