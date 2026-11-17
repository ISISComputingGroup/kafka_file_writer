//! Support for generating fake flatbuffers messages, for
//! benchmarking and testing.

use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::{
    Pu00Message, Pu00MessageArgs, finish_pu_00_message_buffer,
};

pub fn make_pu00(
    reference_time: i64,
    period_number: Option<u32>,
    vetos: Option<u32>,
    proton_charge: Option<f32>,
) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    let args = Pu00MessageArgs {
        source_name: Some(fbb.create_string("unittest")),
        message_id: 0,
        reference_time,
        period_number,
        vetos,
        proton_charge,
    };

    let buf = Pu00Message::create(&mut fbb, &args);
    finish_pu_00_message_buffer(&mut fbb, buf);
    fbb.finished_data().to_vec()
}

pub fn make_ev44(reference_time: i64, time_of_flight: &[i32], pixel_id: &[i32]) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    let args = Event44MessageArgs {
        source_name: Some(fbb.create_string("unittest")),
        message_id: 0,
        reference_time: Some(fbb.create_vector(&[reference_time])),
        reference_time_index: Some(fbb.create_vector(&[0])),
        time_of_flight: Some(fbb.create_vector(time_of_flight)),
        pixel_id: Some(fbb.create_vector(pixel_id)),
    };

    let buf = Event44Message::create(&mut fbb, &args);
    finish_event_44_message_buffer(&mut fbb, buf);
    fbb.finished_data().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use isis_streaming_data_types::{DeserializedMessage, deserialize_message};

    #[test]
    fn test_make_pu00() {
        let pu00 = make_pu00(123456789, Some(123), Some(0xFFFFFFFF), Some(5678.));

        let de = deserialize_message(&pu00).unwrap();

        match de {
            DeserializedMessage::PulseMetadataPu00(pu00) => {
                assert_eq!(pu00.reference_time(), 123456789);
                assert_eq!(pu00.period_number(), Some(123));
                assert_eq!(pu00.vetos(), Some(0xFFFFFFFF));
                assert_eq!(pu00.proton_charge(), Some(5678.));
            }
            _ => panic!("unexpected message type"),
        }
    }

    #[test]
    fn test_make_ev44() {
        let pu00 = make_ev44(123456789, &[1_000_000, 2_000_000], &[123, 456]);

        let de = deserialize_message(&pu00).unwrap();

        match de {
            DeserializedMessage::EventDataEv44(ev44) => {
                assert_eq!(ev44.reference_time().get(0), 123456789);
                assert_eq!(ev44.reference_time_index().get(0), 0);
                assert_eq!(
                    ev44.time_of_flight()
                        .unwrap()
                        .into_iter()
                        .collect::<Vec<_>>(),
                    vec![1_000_000, 2_000_000]
                );
                assert_eq!(
                    ev44.pixel_id().unwrap().into_iter().collect::<Vec<_>>(),
                    vec![123, 456]
                );
            }
            _ => panic!("unexpected message type"),
        }
    }
}
