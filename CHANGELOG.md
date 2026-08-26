# Changelog

## v0.2.0 - 2026-08-26

### Added

- Added Flyway-managed schema migrations for SQLite, MySQL, and H2.
- Added a shared semantic pipeline port contract across Core, Service, and the Studio editor.
- Added automatic migration of persisted legacy port identifiers to the semantic contract.

### Changed

- Existing databases are baselined and validated by Flyway; Hibernate no longer updates the schema.
- Single-record pipeline edges use `records`; Filter uses `matched` and `rejected`; Data Quality uses `clean` and `dirty`.
- Stream Join keeps explicit `left` and `right` input ports, while Route output ports remain configuration-defined.
- Runtime, Service validation, editor behavior, and documentation now use the same port contract.

### Fixed

- Fixed Windows distribution startup when `JAVA_OPTS` is empty.

### Upgrade Notes

- Back up an existing database before upgrading.
- On first startup, Flyway baselines an existing schema and applies the persisted pipeline port migration. Malformed pipeline JSON stops startup and must be corrected before retrying.
- Java 17 is required. `SERVER_PORT` controls the HTTP port; `JAVA_OPTS` is optional and may contain JVM options such as `-Xms512m -Xmx2g`.
