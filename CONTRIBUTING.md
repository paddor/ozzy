# Contributing

Checklist for a change to Ozzy. Check and test details live in
[DEVELOPMENT.md](DEVELOPMENT.md).

- [ ] Read [DESIGN.md](DESIGN.md) before an architecture change.
- [ ] `./scripts/test-all.sh` passes.
- [ ] The `extended` Nextest profile passes when the change touches TCP,
  processes, physical storage, or startup.
- [ ] New tests follow the [test layers](DEVELOPMENT.md#test-layers) and
  [test rules](DEVELOPMENT.md#test-rules).
- [ ] Protocol logic stays sans-I/O. Storage implementations stay behind the
  `ozzy-journal` contracts.
- [ ] Ozzy messages are normal OMQ multipart messages. No Ozzy-specific
  behavior goes into ZMTP.
- [ ] Transport, routing, reconnects, and backpressure stay in OMQ. Socket
  roles follow [doc/PROTOCOL.md](doc/PROTOCOL.md).
- [ ] The owning reference doc is updated when a contract changes.
- [ ] `CHANGELOG.md` gets an entry for user-facing SDK or broker changes
  only.

Report security issues as described in [SECURITY.md](SECURITY.md).
