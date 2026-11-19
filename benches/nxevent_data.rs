/// This benchmarks writes event data to an in-memory file; it only exercises the
/// NXevent_data writer module, and it's corresponding HDF5 write path (including chunking
/// and compression, but not disk write speed).
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use isis_streaming_data_types::deserialize_message;
use itertools::Itertools;
use kafka_file_writer::hdf::file_creator::FileCreator;
use kafka_file_writer::messages::fake::{make_ev44, make_pu00};
use kafka_file_writer::run_writer::run_start_parameters::RunStartParameters;
use kafka_file_writer::stream::traits::KafkaMessageMeta;
use kafka_file_writer::writer_module::{WriterModule, WriterModuleSpec};
use kafka_file_writer::writer_modules::nxevent_data::{NXeventdata, NXeventdataConfig};
use rand::distr::Uniform;
use rand::rng;
use rand::rngs::ThreadRng;
use rand_distr::{Distribution, Normal};
use std::hint::black_box;

fn make_event_stream_messages(num_frames: usize, num_events_per_frame: usize) -> Vec<Vec<u8>> {
    let mut rng: ThreadRng = rng();
    let tof_dist = Normal::new(10_000_000., 1_000_000.).unwrap();
    let detector_dist = Uniform::new(0, 30_000).unwrap();

    let mut messages = vec![];
    for frame_num in 0..num_frames {
        messages.push(make_pu00(
            frame_num as i64 * 20_000_000,
            Some(0),
            Some(0xFFFF),
            Some(1.2345),
        ));
        if num_events_per_frame > 0 {
            // Make a realistic distribution of det_ids and TOFs, otherwise the data
            // 'compresses' extremely well and is no longer representative of the write
            // speed of real data.
            let det_ids: Vec<i32> = (0..num_events_per_frame)
                .map(|_| detector_dist.sample(&mut rng))
                .collect();
            let tofs: Vec<i32> = (0..num_events_per_frame)
                .map(|_| (tof_dist.sample(&mut rng) as i32).max(0))
                .sorted() // real events are sorted in TOF
                .collect();

            messages.push(make_ev44(frame_num as i64 * 20_000_000, &tofs, &det_ids));
        }
    }
    messages
}

fn benchmark_event_writing(c: &mut Criterion) {
    const NUM_FRAMES: usize = 50;

    for num_events_per_frame in [0, 10, 1000, 100_000] {
        for event_compression in [false, true] {
            let messages = make_event_stream_messages(NUM_FRAMES, num_events_per_frame);
            let n_bytes = messages.iter().map(|m| m.len()).sum::<usize>();

            let mut group = c.benchmark_group("event_writing");
            group.throughput(Throughput::ElementsAndBytes {
                elements: NUM_FRAMES as u64,
                bytes: n_bytes as u64,
            });

            group.bench_function(
                BenchmarkId::new(
                    "event_writing",
                    format!(
                        "{} events per frame ({})",
                        num_events_per_frame,
                        if event_compression {
                            "compressed"
                        } else {
                            "uncompressed"
                        }
                    ),
                ),
                |b| {
                    b.iter_batched(
                        || {
                            let file = FileCreator::Memory.create_file("benchmark").unwrap();
                            let group = file.create_group("bench").unwrap();

                            let create_result = NXeventdata::create(
                                group,
                                &RunStartParameters {
                                    instrument_name: "bench".to_string(),
                                    start_time_ms: 0,
                                    stop_time_ms: Some(u64::MAX),
                                    ..Default::default()
                                },
                                NXeventdataConfig {
                                    topic: "someTopic".to_string(),
                                    compression_level: if event_compression {
                                        Some(1)
                                    } else {
                                        None
                                    },
                                },
                            )
                            .unwrap();

                            create_result.module
                        },
                        |mut module| {
                            messages.iter().for_each(|msg| {
                                let meta = KafkaMessageMeta {
                                    topic: "",
                                    timestamp: Some(1),
                                    offset: 0,
                                    partition: 0,
                                    key: None,
                                };
                                let deserialized = deserialize_message(&black_box(msg)).unwrap();
                                module.on_message(&black_box(meta), &deserialized).unwrap();
                            });
                            module.flush().unwrap();
                            module
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }
}

criterion_group! {
    benches,
    benchmark_event_writing
}
criterion_main!(benches);
