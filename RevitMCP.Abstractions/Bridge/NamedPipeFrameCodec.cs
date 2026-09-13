#nullable enable
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RevitMCP.Abstractions.Bridge
{
    /// <summary>
    /// Transport-neutral framing shared by the .NET Framework Revit add-in and
    /// the .NET 8 server. Frames are a four-byte little-endian UTF-8 byte count
    /// followed by one JSON document.
    /// </summary>
    public static class NamedPipeFrameCodec
    {
        public const string ProtocolVersion = "revitmcp.pipe.v1";
        public const int DefaultMaximumFrameBytes = 64 * 1024 * 1024;

        public static async Task WriteJsonAsync(
            Stream stream,
            string json,
            CancellationToken cancellationToken)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (json == null) throw new ArgumentNullException(nameof(json));

            var payload = Encoding.UTF8.GetBytes(json);
            if (payload.Length > DefaultMaximumFrameBytes)
                throw new InvalidDataException("Named-pipe frame exceeds the 64 MiB safety limit.");

            var header = new byte[4];
            header[0] = (byte)(payload.Length & 0xff);
            header[1] = (byte)((payload.Length >> 8) & 0xff);
            header[2] = (byte)((payload.Length >> 16) & 0xff);
            header[3] = (byte)((payload.Length >> 24) & 0xff);

            await stream.WriteAsync(header, 0, header.Length, cancellationToken).ConfigureAwait(false);
            if (payload.Length > 0)
                await stream.WriteAsync(payload, 0, payload.Length, cancellationToken).ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        public static async Task<string> ReadJsonAsync(
            Stream stream,
            CancellationToken cancellationToken,
            int maximumFrameBytes = DefaultMaximumFrameBytes)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (maximumFrameBytes <= 0) throw new ArgumentOutOfRangeException(nameof(maximumFrameBytes));

            var header = new byte[4];
            await ReadExactlyAsync(stream, header, 0, header.Length, cancellationToken).ConfigureAwait(false);
            var length = header[0]
                | (header[1] << 8)
                | (header[2] << 16)
                | (header[3] << 24);

            if (length < 0 || length > maximumFrameBytes)
                throw new InvalidDataException("Invalid named-pipe frame length: " + length + ".");
            if (length == 0) return string.Empty;

            var payload = new byte[length];
            await ReadExactlyAsync(stream, payload, 0, length, cancellationToken).ConfigureAwait(false);
            return Encoding.UTF8.GetString(payload);
        }

        private static async Task ReadExactlyAsync(
            Stream stream,
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken)
        {
            var readTotal = 0;
            while (readTotal < count)
            {
                var read = await stream.ReadAsync(
                    buffer,
                    offset + readTotal,
                    count - readTotal,
                    cancellationToken).ConfigureAwait(false);
                if (read <= 0)
                    throw new EndOfStreamException("Named-pipe connection closed before the frame was complete.");
                readTotal += read;
            }
        }
    }
}
