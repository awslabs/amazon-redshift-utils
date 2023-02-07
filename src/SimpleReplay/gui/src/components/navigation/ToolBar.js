import {SideNavigation} from "@awsui/components-react";
import * as React from 'react';

/**
 * Help Sidebar
 * List of anchor tags
 */
const ToolBar = () => {
    return (
      <SideNavigation
                header={{text: "Help"}}
                items={[
         {
          type: "section",
          text: "Troubleshooting",
          items: [
              {
              type: "link",
                  external:true,
              text: "Common Query Problems",
              href: "https://docs.aws.amazon.com/redshift/latest/dg/queries-troubleshooting.html"
            },
            {
              type: "link", external:true,
              text: "Redshift Spectrum Queries",
              href: "https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-troubleshooting.html"
            },

            {
              type: "link", external:true,
              text: "1023: Serializable Isolation Error",
              href: "https://aws.amazon.com/premiumsupport/knowledge-center/redshift-serializable-isolation/"
            }
          ]
        },
        {
          type: "section",
          text: "Reference",
          items: [
            { type: "link", external:true, text: "Redshift Documentation", href: "https://docs.aws.amazon.com/redshift/index.html" },
            { type: "link",  external:true,text: "Database Developer Guide", href: "https://docs.aws.amazon.com/redshift/latest/dg/welcome.html" },
              { type: "link",  external:true, text: "Cluster Management Guide", href: "https://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html" },

          ]
        }
      ]}
              />
    );
}
export default ToolBar;