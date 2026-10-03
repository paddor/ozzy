# Contributing

Run `./scripts/test-all.sh` before submitting changes. Follow `DESIGN.md`. Keep
protocol logic sans-I/O and storage implementations behind `ozzy-journal`
contracts.

Do not add Ozzy-specific behavior to ZMTP. Ozzy protocol messages are normal
OMQ multipart messages. Keep payload traffic on direct peer links.
