﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClrMachineCode
{
	/// <summary>
	/// Initialize a static, readonly instance of this class on classes containing <see cref="MachineCodeAttribute"/> to trigger automatic initialization just in time.
	/// </summary>
	static public class MachineCodeClassMarker
	{
		public static object Prepare(Type type)
		{
			MachineCodeHandler.PrepareClass(type);
			return null;
		}
	}
}
