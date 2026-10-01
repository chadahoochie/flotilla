use crate::codec::{HEADER_SIZE, MAGIC, PacketHeader};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zerocopy::FromBytes;

/// Pure function to write a framed packet to an async stream.
pub async fn write_packet_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    packet: &[u8],
) -> io::Result<()> {
    writer.write_all(packet).await?;
    writer.flush().await?;
    Ok(())
}

/// Pure function to read an exact framed Flotilla packet from an async stream into a buffer.
pub async fn read_packet_frame<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> io::Result<usize> {
    if buf.len() < HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Destination buffer smaller than header",
        ));
    }

    reader.read_exact(&mut buf[..HEADER_SIZE]).await?;

    let (header, _) = PacketHeader::read_from_prefix(&buf[..HEADER_SIZE])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Failed to read packet header"))?;

    if header.magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid magic bytes: 0x{:08X}", header.magic),
        ));
    }

    let payload_len = header.payload_len as usize;
    let total_len = HEADER_SIZE + payload_len;
    if buf.len() < total_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Payload size {payload_len} exceeds destination buffer"),
        ));
    }

    reader.read_exact(&mut buf[HEADER_SIZE..total_len]).await?;
    Ok(total_len)
}
