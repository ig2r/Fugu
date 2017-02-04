using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    public class FooterCheckVisitor : TableVisitorBase
    {
        public bool? HasValidFooter { get; private set; }

        #region TableVisitorBase overrides

        public override void OnTableFooter()
        {
            HasValidFooter = true;
        }

        public override void OnError(Exception exception)
        {
            HasValidFooter = false;
        }

        #endregion
    }
}
