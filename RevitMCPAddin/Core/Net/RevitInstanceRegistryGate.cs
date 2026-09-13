#nullable enable
using System;
using System.Text;
using System.Threading;

namespace RevitMCPAddin.Core.Net
{
    /// <summary>
    /// Serializes updates to the per-user Revit instance registry across Revit
    /// processes. A process-local lock is insufficient when 5210 and 5211 publish
    /// their registration at the same time.
    /// </summary>
    internal static class RevitInstanceRegistryGate
    {
        private static readonly string MutexName =
            @"Local\RevitMCP_InstanceRegistry_" + SafeName(Environment.UserName);

        public static IDisposable? TryEnter(int timeoutMs)
        {
            Mutex? mutex = null;
            try
            {
                mutex = new Mutex(false, MutexName);
                bool acquired;
                try
                {
                    acquired = mutex.WaitOne(Math.Max(0, timeoutMs));
                }
                catch (AbandonedMutexException)
                {
                    acquired = true;
                }

                if (!acquired)
                {
                    mutex.Dispose();
                    return null;
                }
                return new Lease(mutex);
            }
            catch
            {
                try { mutex?.Dispose(); } catch { }
                return null;
            }
        }

        private static string SafeName(string value)
        {
            var source = string.IsNullOrWhiteSpace(value) ? "default" : value;
            var result = new StringBuilder(source.Length);
            foreach (var ch in source)
                result.Append(char.IsLetterOrDigit(ch) || ch == '-' || ch == '_' ? ch : '_');
            return result.ToString();
        }

        private sealed class Lease : IDisposable
        {
            private Mutex? _mutex;

            public Lease(Mutex mutex)
            {
                _mutex = mutex;
            }

            public void Dispose()
            {
                var mutex = Interlocked.Exchange(ref _mutex, null);
                if (mutex == null) return;
                try { mutex.ReleaseMutex(); } catch { }
                try { mutex.Dispose(); } catch { }
            }
        }
    }
}
