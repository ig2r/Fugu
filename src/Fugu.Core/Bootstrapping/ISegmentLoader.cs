using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    /// <summary>
    /// Provides functionality to verify the integrity of segments and load their contents.
    /// </summary>
    public interface ISegmentLoader
    {
        Task<bool> CheckTableFooterAsync(ITable table);
        Task LoadSegmentAsync(Segment segment, bool verifyChecksums);
    }
}
