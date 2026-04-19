# Security Policy

## Reporting

If you believe you have found a security issue in OCP, please do not open a public issue with full exploit details.

Instead:

1. Open a private security advisory on GitHub if available.
2. Or contact the maintainer privately through GitHub.

Please include:

- affected component
- reproduction steps
- expected impact
- any suggested mitigations

## Scope

Security-sensitive areas include:

- peer identity and handshake
- signed envelopes and replay protection
- secret delivery and redaction
- artifact verification and replication
- trust / autonomy / helper enlistment policy
- recovery and checkpoint handling

## Early Project Note

OCP is still in active development.

That means:

- interfaces may change quickly
- some trust and policy layers are still evolving
- security hardening is ongoing

Responsible reports are still extremely valuable.
