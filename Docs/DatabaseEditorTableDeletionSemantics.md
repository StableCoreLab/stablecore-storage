# Database Editor Table Deletion Semantics

Table deletion in `Tools/DatabaseEditor` is currently treated as an explicit destructive action.

## Current Behavior

- The object explorer and Table menu expose `Delete Selected Table`.
- The action removes the selected table from the database immediately after confirmation.
- The action is not currently part of Undo / Redo history.

## User-Facing Guidance

- Present table deletion as permanent removal.
- Do not describe it as a reversible edit.
- If undoable table deletion is added later, this document should be updated together with the storage journal semantics.
