# Template Organization for DataPusher Plus

This directory contains templates organized to support conditional loading based on configuration options.

## Directory Structure

```
templates/
├── datapusher/              # Core DataPusher Plus templates
├── package/                 # Standard package templates  
├── scheming/               # Standard scheming templates
├── snippets/               # Standard snippets (no DRUF)
└── druf/                   # DRUF-specific template overrides
    ├── package/
    │   └── snippets/
    │       └── package_form.html    # DRUF-modified form stages
    ├── scheming/
    │   └── package/
    │       └── snippets/
    │           └── package_form.html    # DRUF-modified scheming form
    └── snippets/
        └── add_dataset.html     # DRUF-modified dataset creation
```

## How It Works

### Standard Mode (DRUF disabled)
- Only `templates/` is added to template path
- Templates provide standard CKAN behavior
- No resource-first workflow

### DRUF Mode (DRUF enabled)  
- Both `templates/` and `templates/druf/` are added to template path
- DRUF templates override standard ones due to template resolution order
- Enables resource-first workflow

## Template Differences

### Standard Templates
- `snippets/add_dataset.html`: Regular "Add Dataset" links
- `package/snippets/package_form.html`: Standard form stages
- `scheming/package/snippets/package_form.html`: Standard scheming stages

### DRUF Templates  
- `druf/snippets/add_dataset.html`: "Add Dataset" → resource_first.new
- `druf/package/snippets/package_form.html`: Modified stages with "Add data" first
- `druf/scheming/package/snippets/package_form.html`: Modified scheming stages

## Configuration

Enable DRUF templates with:
```ini
ckanext.datapusher_plus.enable_druf = true
```

The plugin will automatically:
1. Load DRUF blueprints (`resource_first.new` endpoint)
2. Add DRUF template directory for overrides
3. Log the enabled status
