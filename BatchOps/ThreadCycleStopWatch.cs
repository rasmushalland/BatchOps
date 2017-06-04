using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using System.Threading.Tasks;

namespace BatchOps
{
	public class ThreadCycleStopWatch
	{
		[DllImport("Kernel32.dll")]
		static extern int QueryThreadCycleTime(int threadHandle, out ulong cycles);
		[DllImport("Kernel32.dll")]
		static extern int GetCurrentThread();

		private ThreadCycleStopWatch()
		{
		}

		[SecuritySafeCritical]
		static int GetCurrentThreadWrapper()
		{
			return GetCurrentThread();
		}

		ulong _start;
		int _startThread;


		[SecuritySafeCritical]
		private static ulong GetQueryThreadCycleTime(int threadHandle)
		{
			ulong cycleTime;
			var res = QueryThreadCycleTime(threadHandle, out cycleTime);
			if (res == 0)
				throw new Exception("QueryThreadCycleTime error result " + res);
			return cycleTime;
		}

		[SecuritySafeCritical]
		static public ThreadCycleStopWatch StartNew()
		{
			int thread = GetCurrentThreadWrapper();
			ulong l = GetQueryThreadCycleTime(thread);
			return new ThreadCycleStopWatch { _start = l, _startThread = thread };
		}

		[SecuritySafeCritical]
		public long GetCurrentCycles()
		{
			int thread = GetCurrentThreadWrapper();
			if (thread != _startThread)
				throw new InvalidOperationException("This ThreadCycleStopWatch belongs to another thread (current thread=" + thread + ", start thread=" + _startThread + ").");
			ulong stopCount = GetQueryThreadCycleTime(thread);

			try
			{
				if (stopCount < _start)
					return -1;
				return (long)(stopCount - _start);
			}
			catch (Exception e)
			{
				throw new OverflowException("start: " + _start + ", stop: " + stopCount, e);
			}
		}

		public override string ToString()
		{
			return "CPU: " + GetCurrentCycles().ToString("###,###,###,###,###");
		}
	}
}
