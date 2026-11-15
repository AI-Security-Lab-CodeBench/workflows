type Selection = {readonly scope: string; readonly id: string};
export type InspectorSelection = {readonly revision: number; readonly selection?: Selection};
type Action =
  | {readonly type: 'select'; readonly selection: Selection}
  | {readonly type: 'close'}
  | {readonly type: 'advance'; readonly revision: number; readonly selection: Selection | undefined};

export function inspectorSelection(state: InspectorSelection, action: Action): InspectorSelection {
  switch (action.type) {
    case 'select':
      return {revision: state.revision + 1, selection: action.selection};
    case 'close':
      return {revision: state.revision + 1};
    case 'advance':
      if (state.revision !== action.revision || !state.selection) {return state;}
      return action.selection
        ? {revision: state.revision + 1, selection: action.selection}
        : {revision: state.revision + 1};
  }
}
