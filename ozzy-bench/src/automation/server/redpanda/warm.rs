//! Initialize Redpanda's lazy internal ID allocator before scheduled arrivals.
//! Kafka `InitProducerId` v0 allocates an ID without producing any records.
use crate::automation::Result;
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

pub(super) fn allocate(endpoint: &str) -> Result<Option<i64>> {
    let mut socket =
        TcpStream::connect_timeout(&endpoint.parse::<SocketAddr>()?, Duration::from_secs(2))?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    socket.set_write_timeout(Some(Duration::from_secs(2)))?;
    let client = b"ozzy-fixture";
    let mut request = Vec::new();
    request.extend_from_slice(&22_i16.to_be_bytes()); // API key
    request.extend_from_slice(&0_i16.to_be_bytes()); // API version
    request.extend_from_slice(&1_i32.to_be_bytes()); // Correlation ID
    request.extend_from_slice(&i16::try_from(client.len())?.to_be_bytes());
    request.extend_from_slice(client);
    request.extend_from_slice(&(-1_i16).to_be_bytes()); // No transaction ID
    request.extend_from_slice(&10_000_i32.to_be_bytes());
    socket.write_all(&i32::try_from(request.len())?.to_be_bytes())?;
    socket.write_all(&request)?;
    let mut length = [0; 4];
    socket.read_exact(&mut length)?;
    if i32::from_be_bytes(length) != 20 {
        return Err("invalid InitProducerId response size".into());
    }
    let mut reply = [0; 20];
    socket.read_exact(&mut reply)?;
    decode(&reply)
}

fn decode(reply: &[u8; 20]) -> Result<Option<i64>> {
    if i32::from_be_bytes(reply[..4].try_into()?) != 1 {
        return Err("invalid InitProducerId correlation".into());
    }
    let error = i16::from_be_bytes(reply[8..10].try_into()?);
    match error {
        0 => {
            let id = i64::from_be_bytes(reply[10..18].try_into()?);
            let epoch = i16::from_be_bytes(reply[18..].try_into()?);
            if id < 0 || epoch < 0 {
                return Err("invalid allocated producer ID".into());
            }
            Ok(Some(id))
        }
        // The allocator's internal topic/coordinator may still be starting.
        3 | 7 | 14 | 15 | 16 => Ok(None),
        _ => Err(format!("InitProducerId failed with Kafka error {error}").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::TcpListener, thread};

    #[test]
    fn probe_is_record_free_and_bounds_the_response_before_reading_it() {
        for size in [20_i32, i32::MAX] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = [0; 32];
                socket.read_exact(&mut request).unwrap();
                assert_eq!(i32::from_be_bytes(request[..4].try_into().unwrap()), 28);
                assert_eq!(&request[4..8], &[0, 22, 0, 0]);
                assert_eq!(&request[14..26], b"ozzy-fixture");
                assert_eq!(&request[26..28], &[255, 255]);
                socket.write_all(&size.to_be_bytes()).unwrap();
                if size == 20 {
                    let mut reply = [0; 20];
                    reply[3] = 1;
                    reply[17] = 42;
                    socket.write_all(&reply).unwrap();
                }
            });
            let result = allocate(&endpoint);
            server.join().unwrap();
            if size == 20 {
                assert_eq!(result.unwrap(), Some(42));
            } else {
                assert!(result.unwrap_err().to_string().contains("response size"));
            }
        }
    }

    #[test]
    fn only_startup_errors_retry_and_invalid_ids_are_rejected() {
        let mut reply = [0; 20];
        reply[3] = 1;
        for error in [3_i16, 7, 14, 15, 16] {
            reply[8..10].copy_from_slice(&error.to_be_bytes());
            assert_eq!(decode(&reply).unwrap(), None);
        }
        reply[9] = 29; // Authorization failure is not readiness.
        assert!(decode(&reply).is_err());
        reply[9] = 0;
        reply[10..18].copy_from_slice(&(-1_i64).to_be_bytes());
        assert!(decode(&reply).is_err());
    }
}
