//! # Kafka file writer
//!
//! Listen to Kafka, write the data into NeXus files.
//!
//! The data may include things like event-mode data, histogrammed views of the event-mode data,
//! sample-environment block data, run metadata, static datasets describing instrument or sample
//! state.
//!
//! ## Architecture overview
//!
//! The architecture of this filewriter is loosely based on the
//! [ESS filewriter](https://github.com/ess-dmsc/kafka-to-nexus). The following design elements are
//! architecturally similar to the ESS filewriter:
//! - Pooled architecture driven by a Kafka topic
//! - Configurable `nexus_template`, sent as part of the run start message
//! - Writer-modules which describe how to write Kafka messages to NeXus files
//!
//! However, there are also a number of areas where we diverge from the ESS design:
//! - Our writer modules may receive multiple schemas, perhaps from multiple topics.
//!   For example, an event-data writer may need `ev44`, `pu00`, and `vc00` messages as inputs,
//!   from `_events` and `_vetoConfig` topics.
//! - Our messages may be delivered to multiple writer-modules (for example, a histogramming module
//!   and also an event-writing module).
//! - Support for intermediate files (autosave files & explicit store operations).
//!
//! ## File-writer
//!
//! The file-writer is run using:
//! ```shell
//! kafka_file_writer --config config.toml
//! ```
//! Or for a development build:
//! ```shell
//! cargo run --bin kafka_file_writer -- --config config.toml
//! ```
//!
//! ## Configuration
//!
//! Global configuration (e.g. Kafka broker settings) are specified in `config.toml`.
//!
//! Per-run configuration, for example NeXus structure, comes via the run start message.
//!
//! ## Nexus structures
//!
//! The file-structure written by this filewriter is defined dynamically, by the `nexus_structure`
//! field of a [run start message](https://github.com/ISISComputingGroup/streaming-data-types/blob/master/schemas/pl72_run_start.fbs).
//!
//! For example, the `nexus_structure` defines:
//! - Whether event-mode data is written, and if so, where in the file it is written to
//! - Whether histogram-mode data is written
//! - Which blocks are written
//! - Which static datasets are written
//!
//! The NeXus structures expected by this program are inspired by, but differ from, the ESS'
//! NeXus structures.
//!
//! Some example structures are available in the `structures/` directory.
//!
//! A standalone `structure_verify` executable is available, which attempts to instantiate an
//! in-memory file with the specified structure, to verify it instantiates correctly. Run it with:
//! ```shell
//! structure_verify --file structures/isis.json
//! ```
//! Or for a development build:
//! ```shell
//! cargo run --bin structure_verify -- --file structures/isis.json
//! ```
//!
//! ## Threading model
//!
//! This filewriter is single-threaded, but is scalable via different filewriter processes writing
//! different runs (potentially on different servers) at the same time.
//!
//! The hdf5 library, even when built in a threadsafe configuration, can only write to one dataset
//! at a time (threadsafe builds guard all library calls with a global lock). Therefore, there is
//! no significant benefit to HDF5 write speeds when multithreading.
//!
//! Writer modules may use multithreading internally for parallelizable work.
pub mod config;
pub mod error;
pub mod file_writer;
pub mod hdf;
pub mod run_writer;
pub mod stream;
pub mod subscription;
pub mod writer_module;
pub mod writer_module_factories;
pub mod writer_modules;
