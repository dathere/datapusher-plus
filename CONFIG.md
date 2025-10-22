# DataPusher Plus Configuration

## Optional Features

DataPusher Plus includes some optional features that can be enabled through configuration. These features are disabled by default to ensure compatibility with different CKAN versions.

### IFormRedirect Support

The IFormRedirect interface provides custom redirect behavior after dataset and resource form submissions. This interface is only available in certain CKAN branches and is not yet merged into the main CKAN codebase.

**Note**: IFormRedirect methods are only defined when this feature is enabled, keeping the plugin completely clean when disabled.

**Configuration:**
```ini
# Enable IFormRedirect functionality (default: false)
ckanext.datapusher_plus.enable_form_redirect = true
```

**What it does:**
- **Dynamically adds IFormRedirect methods** only when enabled
- Provides custom redirect URLs after dataset/resource creation or editing
- Redirects to dataset page after dataset metadata submission
- Redirects to resource view after resource editing
- Allows "add another resource" workflow
- **Works best with DRUF** for complete resource-first workflow

**Requirements:**
- CKAN version with IFormRedirect interface support
- If the interface is not available, the feature will be automatically disabled with a warning
- **Recommended**: Enable together with DRUF for optimal resource-first experience

### DRUF (Dataset Resource Upload First) Support

DRUF allows users to upload resources before creating the dataset metadata, providing a resource-first workflow.

**Configuration:**
```ini
# Enable DRUF functionality (default: false)  
ckanext.datapusher_plus.enable_druf = true
```

**What it does:**
- Adds a `/resource-first/new` endpoint
- Creates a temporary dataset and redirects to resource upload
- Useful for workflows where users want to upload data files first
- **Overrides templates**: Modifies "Add Dataset" buttons and form stages to support resource-first workflow

**Template Overrides:**
When DRUF is enabled, the following templates are overridden:
- `snippets/add_dataset.html`: Changes "Add Dataset" to redirect to resource upload
- `package/snippets/package_form.html`: Modifies form stages to show "Add data" first
- `scheming/package/snippets/package_form.html`: Modifies scheming form stages

**Requirements:**
- No special CKAN version requirements
- Works with standard CKAN installations
- Compatible with ckanext-scheming

## Example Configuration

Add these lines to your CKAN configuration file (e.g., `/etc/ckan/default/ckan.ini`):

```ini
# Enable DRUF (Dataset Resource Upload First) workflow
ckanext.datapusher_plus.enable_druf = true

# Enable IFormRedirect for better form redirects (recommended with DRUF)
ckanext.datapusher_plus.enable_form_redirect = true
```

**Recommended combinations:**
- **Standard mode**: Both disabled (default) - maintains standard CKAN behavior
- **Resource-first workflow**: Both enabled - complete resource-first experience
- **DRUF only**: Only `enable_druf = true` - resource-first without custom redirects

## Template Organization

DataPusher Plus uses a conditional template loading system to avoid conflicts when optional features are disabled:

- **Base templates** (`templates/`): Always loaded, provides standard DataPusher Plus functionality
- **DRUF templates** (`templates/druf/`): Only loaded when `enable_druf = true`, overrides default dataset creation workflow

This ensures that when DRUF is disabled, your CKAN installation maintains completely standard behavior without any template modifications.

## Backwards Compatibility

When these features are disabled (default), DataPusher Plus maintains full backwards compatibility with standard CKAN installations. The plugin will automatically detect if required interfaces are available and disable features gracefully if they are not supported.

## Logging

The plugin will log the status of these features:
- Info messages when features are successfully enabled
- Warning messages when features are configured but not available
- Debug messages for DRUF blueprint registration

Check your CKAN logs to verify the status of these optional features.
