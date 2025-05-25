# Plex Database Tool - Usage Guide

This tool analyzes your Plex database to show recently modified media files and optionally creates a modified copy where the "added date" matches the file modification time.

## Quick Start

### View Recent Media (Read-Only)
To simply view the 30 most recently modified files without making any changes:
```bash
python plex_tool.py
```

### View More Entries
To see the 50 most recently modified files:
```bash
python plex_tool.py --limit 50
```

### Specify Custom Database Path
If your Plex database is in a different location:
```bash
python plex_tool.py --db "/path/to/your/plex/database.db"
```

## Command Line Arguments

| Argument | Description | Default Value |
|----------|-------------|---------------|
| `--db` | Path to Plex database file | `~/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db` |
| `--limit` | Number of entries to show | `30` |
| `--write` | Create modified database copy | `False` (read-only) |
| `--output` | Output path for modified database | `~/plex_modified_library.db` |

## Usage Examples

### 1. Basic Read-Only Usage (Most Common)
```bash
# View 30 most recent files
python plex_tool.py

# View 100 most recent files  
python plex_tool.py --limit 100

# View 10 most recent files
python plex_tool.py --limit 10
```

### 2. Custom Database Location
```bash
# If your Plex database is elsewhere
python plex_tool.py --db "/home/user/custom/plex.db"

# View 50 entries from custom location
python plex_tool.py --db "/mnt/plex/database.db" --limit 50
```

### 3. Creating Modified Database (Advanced)
```bash
# Create a copy with synced added dates
python plex_tool.py --write

# Create copy with custom output location
python plex_tool.py --write --output "/backup/plex_fixed.db"

# Process 100 entries and create modified copy
python plex_tool.py --limit 100 --write
```

## Sample Output

When you run the tool in read-only mode, you'll see output like this:

```
Top 30 Entries by File Modification Time:
================================================================================
                               title  library_section_id     added_at        mtime
                        Movie Title                      1  2024-01-15 10:30:00  2024-05-20 14:22:15
                     Another Movie                        1  2024-02-10 09:15:00  2024-05-19 16:45:30
                          TV Show                         2  2024-03-01 11:00:00  2024-05-18 20:10:45
```

## What the Tool Does

### Read-Only Mode (Default)
- Connects to your Plex database
- Finds the most recently modified media files
- Shows a table with:
  - **title**: Name of the movie/show
  - **library_section_id**: Which Plex library it belongs to
  - **added_at**: When Plex thinks it was added
  - **mtime**: When the file was actually last modified
- Makes NO changes to your database

### Write Mode (--write flag)
- Does everything from read-only mode
- Creates a backup copy of your database
- Updates the copy so `added_at = mtime` for the processed entries
- Your original database remains untouched

## Safety Notes

⚠️ **Important**: This tool never modifies your original Plex database. When using `--write`, it always creates a copy first.

✅ **Safe to use**: The default read-only mode is completely safe and makes no changes whatsoever.

## Troubleshooting

### "Database not found"
```bash
# Check if your database path is correct
ls -la "~/Plex Media Server/Plug-in Support/Databases/"

# Use the correct path
python plex_tool.py --db "/actual/path/to/com.plexapp.plugins.library.db"
```

### "No entries found"
- Your database might be empty
- Try increasing the limit: `--limit 100`
- Check if your media files have valid modification times

### Permission Issues
```bash
# Make sure you can read the database
chmod +r "~/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db"
```

## Common Use Cases

1. **Check recent additions**: See what files were recently added/modified
2. **Debug Plex import issues**: Understand why dates don't match
3. **Audit media library**: Review file modification patterns
4. **Fix added dates**: Create corrected database where added dates match file times

## Getting Help

Run with `--help` to see all options:
```bash
python plex_tool.py --help
```