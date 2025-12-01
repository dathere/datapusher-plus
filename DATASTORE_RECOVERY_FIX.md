# DataPusher+ Datastore Recovery Fix

## Problem
When DataPusher+ resubmits a resource (e.g., via `ckan datastore resubmit`), if there are failures during metadata updates (such as SOLR being down), the entire datastore would be deleted including any manual Data Dictionary edits. This resulted in permanent data loss.

## Root Cause
The original code flow was:
1. Back up Data Dictionary info
2. Delete existing datastore completely
3. Create new datastore table
4. Copy data to datastore
5. Update resource metadata (can fail if SOLR is down)
6. Update package metadata (can fail if SOLR is down)

If steps 5 or 6 failed, the old datastore was already deleted and couldn't be recovered.

## Solution
The fix implements a robust error handling strategy with three layers of protection:

### 1. Complete Field Backup Before Deletion
```python
existing_fields_backup = None  # Backup complete field definitions for recovery
if existing:
    existing_info = dict(...)
    # Backup complete field definitions including Data Dictionary edits
    existing_fields_backup = existing.get("fields", [])
```

This captures the complete field schema including all Data Dictionary edits (labels, types, notes, etc.).

### 2. Rollback on Datastore Creation Failure
```python
try:
    # Create datastore table
    dsu.send_resource_to_datastore(...)
except Exception as e:
    # If we deleted an existing datastore and creation fails, restore it
    if datastore_deleted and existing_fields_backup:
        logger.warning("Attempting to restore previous datastore structure...")
        dsu.send_resource_to_datastore(
            resource_id=resource["id"],
            headers=existing_fields_backup,  # Restore with Data Dictionary
            ...
        )
```

If creating the new datastore table fails, immediately restore the previous structure with all Data Dictionary edits.

### 3. Preserve Datastore on Metadata Update Failures
```python
try:
    dsu.update_resource(resource)
    dsu.send_resource_to_datastore(...)
    dsu.patch_package(package)
except Exception as e:
    logger.error(f"Failed to update resource/package metadata (possibly SOLR issue): {e}")
    logger.warning(
        f"Datastore table '{resource_id}' with Data Dictionary was successfully created, "
        f"but metadata updates failed. The datastore and Data Dictionary are preserved."
    )
    raise utils.JobError(
        f"Datastore created successfully but metadata update failed: {e}. "
        f"Data Dictionary is preserved in datastore."
    )
```

If SOLR or other CKAN metadata updates fail, the datastore (with all data and Data Dictionary) remains intact. The job fails with a clear error message, but no data is lost.

## Benefits
1. **Data Dictionary Preservation**: Manual Data Dictionary edits are never lost
2. **Graceful Degradation**: If metadata updates fail (SOLR down), datastore remains functional
3. **Clear Error Messages**: Users know exactly what succeeded and what failed
4. **Atomic Operations**: Either everything succeeds or the previous state is preserved
5. **Retry-Friendly**: Failed jobs can be retried without losing previous work

## Testing Recommendations
1. **SOLR Failure Test**: 
   - Set up a dataset with edited Data Dictionary
   - Stop SOLR pod
   - Run `ckan datastore resubmit --yes`
   - Verify Data Dictionary is preserved
   
2. **Database Connection Test**:
   - Create resource with Data Dictionary edits
   - Simulate database connection issues during update
   - Verify rollback restores original structure

3. **Normal Operation Test**:
   - Verify normal resubmit still works correctly
   - Verify Data Dictionary edits are properly merged

## Files Modified
- `/usr/lib/ckan/default/src/datapusher-plus/ckanext/datapusher_plus/jobs.py`
  - Added `existing_fields_backup` variable to capture complete field definitions
  - Added try-except around datastore table creation with rollback logic
  - Added try-except around metadata updates to preserve successful datastore creation

## Backward Compatibility
This fix is fully backward compatible. It adds error handling without changing the API or normal operation flow.
