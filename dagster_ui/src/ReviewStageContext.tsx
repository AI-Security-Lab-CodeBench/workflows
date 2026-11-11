import {createContext, useContext, useState} from 'react';
import type {ReactNode} from 'react';
import {ApiError} from './api';
import {reviewStagesSchema} from './models';
import type {ReviewStage} from './models';
import {useResource} from './useResource';

const ReviewStagesContext = createContext<{
  readonly stages: readonly ReviewStage[] | undefined;
  readonly error: unknown;
  readonly reload: () => void;
} | undefined>(undefined);

export function ReviewStagesProvider({children}: {readonly children: ReactNode}) {
  const [refresh, setRefresh] = useState(0);
  const result = useResource('/review-stages', reviewStagesSchema, undefined, refresh, 'GET');
  return <ReviewStagesContext.Provider value={{
    stages: result?.data?.stages, error: result?.error,
    reload: () => setRefresh((value) => value + 1),
  }}>{children}</ReviewStagesContext.Provider>;
}

export function useReviewStages() {
  const registry = useContext(ReviewStagesContext);
  if (!registry) {
    throw new ApiError(500, 'missing_review_registry', 'Review stages require the native asset provider.');
  }
  return registry;
}
