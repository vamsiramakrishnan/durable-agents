fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../proto/tape.proto");
    // `madsim` is the custom cfg flag that activates the deterministic
    // simulator inside the `madsim` crate (see Phase 2.5 in
    // `design-principles/chaos.md`). Register it so the unexpected-cfg
    // lint stays quiet under `RUSTFLAGS='--cfg madsim'`.
    println!("cargo:rustc-check-cfg=cfg(madsim)");
    // tonic 0.14 split proto compilation out of tonic-build into
    // tonic-prost-build. build_server defaults to true; we only need to
    // disable the client side.
    tonic_prost_build::configure()
        .build_client(false)
        .compile_protos(&["../proto/tape.proto"], &["../proto"])?;
    Ok(())
}
