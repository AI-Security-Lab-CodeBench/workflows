import {useEffect, useState} from 'react';

const key = 'codebench-human-reviewer';

export function useReviewer() {
  const [reviewer, setReviewer] = useState('');
  const [storageError, setStorageError] = useState(false);
  useEffect(() => {
    try {setReviewer(window.localStorage.getItem(key) ?? '');}
    catch (error) {
      if (!(error instanceof DOMException)) {throw error;}
      setStorageError(true);
    }
  }, []);
  function change(value: string) {
    setReviewer(value);
    try {window.localStorage.setItem(key, value);}
    catch (error) {
      if (!(error instanceof DOMException)) {throw error;}
      setStorageError(true);
    }
  }
  return {reviewer, change, storageError};
}
