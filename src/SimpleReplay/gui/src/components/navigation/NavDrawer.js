import {SideNavigation} from "@awsui/components-react";
import * as React from 'react';

/**
 * Navigation Sidebar
 * List of anchor tags
 */
const Nav = () => {
    return (
      <SideNavigation
        header={"Simple Replay Analysis"}
        items={[
            {type: "link", text: "Home", href: "/"},
            {type: "link", text: "Analysis", href: "/analysis#analysis"},
            {type: "link", text: "Validation", href: "/analysis#validation"},
            {type: "link", text: "Resources", href: "/analysis#resources"},
        ]}
      />
    );
}
export default Nav;