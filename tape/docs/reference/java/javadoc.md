# Javadoc

The full Javadoc is built by `mvn javadoc:javadoc` and copied to
`reference/java/javadoc/` at site-build time.

[:material-open-in-new: **Open the Javadoc**](javadoc/index.html){ .md-button .md-button--primary target="_blank" }

If you'd rather have the Javadoc inline, you can `mvn javadoc:jar` locally and unpack the result
under `src/main/javadoc/` — the build script will pick it up automatically.
