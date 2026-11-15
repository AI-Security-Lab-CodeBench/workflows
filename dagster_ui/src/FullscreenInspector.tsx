import {Button, Dialog, DialogHeader} from '@dagster-io/ui-components';
import {useEffect, useId, useRef} from 'react';
import type {ReactNode} from 'react';
import styles from './ui.module.css';

export function FullscreenInspector({isOpen, title, onClose, children}: {
  readonly isOpen: boolean; readonly title: string; readonly onClose: () => void;
  readonly children: ReactNode;
}) {
  const titleId = useId();
  const header = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!isOpen) {return;}
    const closeOnEscape = (event: KeyboardEvent) => {
      const dialogs = document.querySelectorAll('[role="dialog"]');
      const top = dialogs.item(dialogs.length - 1);
      if (event.key === 'Escape' && !event.defaultPrevented && header.current && top?.contains(header.current)) {
        event.preventDefault();
        event.stopPropagation();
        onClose();
      }
    };
    document.addEventListener('keydown', closeOnEscape);
    return () => document.removeEventListener('keydown', closeOnEscape);
  }, [isOpen, onClose]);
  return <Dialog isOpen={isOpen} className={styles.dialog} onClose={onClose}
    aria-labelledby={titleId} canEscapeKeyClose={false} canOutsideClickClose={false}
    onOpened={() => header.current?.focus({preventScroll: true})}
    enforceFocus shouldReturnFocusOnClose>
    <div ref={header} tabIndex={-1} className={styles.dialogHeader}>
      <DialogHeader label={<span id={titleId}>{title}</span>}
        right={<Button onClick={onClose}>Close</Button>} />
    </div>
    <div className={styles.dialogBody}>{children}</div>
  </Dialog>;
}
