using System;
using System.Collections.Generic;
using System.Text;

namespace DapperBulkQueries.Common;

/// <summary>
/// 
/// </summary>
public enum OnConflict
{
    /// <summary>
    /// Default behavior, throws an error if a conflict is detected.
    /// </summary>
    Error = 0,
    /// <summary>
    /// Do nothing with the conflicting rows.
    /// </summary>
    DoNothing = 1,
    /// <summary>
    /// Update the conflicting rows with the new values.
    /// </summary>
    // Update = 2, // not implemented yet
}
