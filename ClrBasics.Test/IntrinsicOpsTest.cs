﻿using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using NUnit.Framework;

namespace ClrBasics.Test
{
	[TestFixture]
	public class IntrinsicOpsTest : UnitTestBase
	{
		private static readonly object Dummy = MachineCodeClassMarker.EnsurePrepared(typeof(IntrinsicOps));

		private static readonly bool TestX64Code = Environment.Is64BitProcess;

		[TestFixtureSetUp]
		public void SetUp()
		{
			//return;
			MachineCodeHandler.TraceSource.Listeners.Add(new ConsoleTraceListener());
			MachineCodeHandler.TraceSource.Switch.Level = SourceLevels.All;

			MachineCodeClassMarker.EnsurePrepared(typeof(IntrinsicOps));
		}

		[Test]
		public void PopulationCount64()
		{
			AreEqual(4, IntrinsicOps.PopulationCount((3L << 55) | (3 << 5)));

			AreEqual(2, IntrinsicOps.PopulationCountSoftware(3L));
			AreEqual(2, IntrinsicOps.PopulationCountSoftware(3L << 5));
			AreEqual(2, IntrinsicOps.PopulationCountSoftware(3L << 55));
			AreEqual(4, IntrinsicOps.PopulationCountSoftware((3L << 55) | (3 << 5)));

			if (TestX64Code)
			{
				AreEqual(2, IntrinsicOps.PopulationCountReplaced(3L));
				AreEqual(2, IntrinsicOps.PopulationCountReplaced(3L << 5));
				AreEqual(2, IntrinsicOps.PopulationCountReplaced(3L << 55));
				AreEqual(4, IntrinsicOps.PopulationCountReplaced((3L << 55) | (3 << 5)));
			}
		}

		[Test]
		public void PopulationCount32()
		{
			AreEqual(4, IntrinsicOps.PopulationCount((3 << 25) | (3 << 5)));

			AreEqual(2, IntrinsicOps.PopulationCountSoftware(3));
			AreEqual(2, IntrinsicOps.PopulationCountSoftware(3 << 5));
			AreEqual(2, IntrinsicOps.PopulationCountSoftware(3 << 25));
			AreEqual(4, IntrinsicOps.PopulationCountSoftware((3 << 25) | (3 << 5)));

			if (TestX64Code)
			{
				AreEqual(2, IntrinsicOps.PopulationCountReplaced(3));
				AreEqual(2, IntrinsicOps.PopulationCountReplaced(3 << 5));
				AreEqual(2, IntrinsicOps.PopulationCountReplaced(3 << 25));
				AreEqual(4, IntrinsicOps.PopulationCountReplaced((3 << 25) | (3 << 5)));
			}
		}

		[Test]
		public void SwapBytes64()
		{
			AreEqual(0x0102030405060708UL, IntrinsicOps.SwapBytes(0x0807060504030201));

			AreEqual(0x0807060504030201UL, IntrinsicOps.SwapBytesSoftware(0x0102030405060708));
			AreEqual(0x0102030405060708UL, IntrinsicOps.SwapBytesSoftware(0x0807060504030201));

			if (TestX64Code)
			{
				AreEqual(0x0807060504030201UL, IntrinsicOps.SwapBytesReplaced(0x0102030405060708));
				AreEqual(0x0102030405060708UL, IntrinsicOps.SwapBytesReplaced(0x0807060504030201));
			}
		}

		[Test]
		public void SwapBytes32()
		{
			AreEqual(0x01020304U, IntrinsicOps.SwapBytes(0x04030201));

			AreEqual(0x04030201U, IntrinsicOps.SwapBytesSoftware(0x01020304));
			AreEqual(0x01020304U, IntrinsicOps.SwapBytesSoftware(0x04030201));

			if (TestX64Code)
			{
				AreEqual(0x04030201U, IntrinsicOps.SwapBytesReplaced(0x01020304));
				AreEqual(0x01020304U, IntrinsicOps.SwapBytesReplaced(0x04030201));
			}
		}

		[Test]
		public void CPUID()
		{
			var ecx = IntrinsicOps.CPUIDEcxReplaced();
			Console.WriteLine("{0:x}", ecx);
		}

		[Test]
		public void Pause()
		{
			IntrinsicOps.Pause();
		}
	}
}