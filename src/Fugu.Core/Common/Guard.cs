using System;
using System.Runtime.CompilerServices;

namespace Fugu.Common
{
    /// <summary>
    /// Static methods to facilitate parameter checking.
    /// </summary>
    public static class Guard
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NotNull<T>(T parameter, string parameterName)
            where T : class
        {
            if (ReferenceEquals(parameter, null))
            {
                throw new ArgumentNullException(parameterName);
            }
        }
    }
}
