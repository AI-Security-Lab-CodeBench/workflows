import {NavCollapseContext} from '@dagster-io/ui-core/app/navigation/NavCollapseProvider';
import {useContext, useEffect, useRef} from 'react';

export function useNarrowNavigation(enabled: boolean) {
  const context = useContext(NavCollapseContext);
  const current = useRef(context);
  const narrowSeen = useRef(false);
  current.current = context;
  useEffect(() => {
    if (!enabled) {return;}
    const media = window.matchMedia('(max-width: 720px)');
    const collapse = () => {
      if (!media.matches) {
        narrowSeen.current = false;
      } else if (!narrowSeen.current) {
        narrowSeen.current = true;
        if (current.current.canCollapse && !current.current.isCollapsed) {
          current.current.toggleCollapsed();
        }
      }
    };
    collapse();
    media.addEventListener('change', collapse);
    return () => media.removeEventListener('change', collapse);
  }, [enabled]);
}
