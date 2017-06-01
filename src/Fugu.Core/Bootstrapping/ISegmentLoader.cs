using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    /// <summary>
    /// Provides functionality to load segment contents into the index.
    /// </summary>
    public interface ISegmentLoader
    {
        Task<bool> TryLoadSegmentAsync(Segment segment, bool requireValidFooter);
    }
}
