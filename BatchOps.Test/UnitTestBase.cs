using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace BatchOps.Test
{
	public class UnitTestBase
	{
		protected static bool outputMarkdownTable = false;

		public static void AreEqual<T>(T expected, T actual) =>
			Assert.AreEqual(expected, actual, "expected 0x{0:X}, got 0x{1:X}.", expected, actual);

		public static void AreEqualHex<T>(T expected, T actual) =>
			Assert.AreEqual(expected, actual, "expected 0x{0:X}, got 0x{1:X}.", expected, actual);

		public static void AreNotEqual<T>(T expected, T actual) =>
			Assert.AreNotEqual(expected, actual);

		public static void AreEqualSequences<T>(IEnumerable<T> expected, IEnumerable<T> actual) =>
			Assert.AreEqual(expected is IList<T> ? expected : expected.ToList(), actual is IList<T> ? actual : actual.ToList());

		public static void AreEqualSequences<T>(IEnumerable<T> expected, IEnumerable<T> actual, string message) =>
			Assert.AreEqual(expected is IList<T> ? expected : expected.ToList(), actual is IList<T> ? actual : actual.ToList(), message);

		public static void IsFalse(bool condition) => Assert.IsFalse(condition);
		public static void IsTrue(bool condition) => Assert.IsTrue(condition);

		[DebuggerStepThrough]
		public static T Throws<T>(Action action) where T : Exception
		{
			try
			{
				action();
			}
			catch (T ex)
			{
				return ex;
			}
			Assert.Fail("Exception of type " + typeof(T).Name + " was expected.");
			return null;
		}

		protected static void BM(string name, Func<long> doit)
		{
			doit();
			long iterations = 0;
			long elapsed = 0;

			bool avoidGC = false;
			if (avoidGC)
			{
				for (var i = 0; i < 10; i++)
				{
					var gcsBefore = GC.CollectionCount(0);
					var sw = ThreadCycleStopWatch.StartNew();
					iterations = doit();
					elapsed = sw.GetCurrentCycles();

					var gcsAfter = GC.CollectionCount(0);
					if (gcsAfter == gcsBefore)
						break;
					if (i > 8)
						throw new ApplicationException("Can't get measurement without GC.");
				}
			}
			else
			{
				var sw = ThreadCycleStopWatch.StartNew();
				iterations = doit();
				elapsed = sw.GetCurrentCycles();
			}
			if (outputMarkdownTable)
				Console.WriteLine("|" + name + " | " + (elapsed / iterations) + " |");
			else
				Console.WriteLine($"{name}: {elapsed / iterations} cycles/iter.");
		}

		[MethodImpl(MethodImplOptions.NoInlining)]
		protected static void AssertSideeffect(long sideeffect)
		{
			// nothing.
		}
	}
}
