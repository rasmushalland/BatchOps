using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BatchOps
{
	static class Utilities
	{
		public static string StringJoin(this IEnumerable<string> strings, string separator)
		{
			return string.Join(separator, strings);
		}
	}
}
