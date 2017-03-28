using System;

namespace Fugu.Common
{
    public static class Guard
    {
        public static void NotNull<T>(T parameter, string parameterName)
            where T : class
        {
            if (ReferenceEquals(parameter, null))
            {
                throw string.IsNullOrEmpty(parameterName)
                    ? new ArgumentNullException()
                    : new ArgumentNullException(parameterName);
            }
        }
    }
}
