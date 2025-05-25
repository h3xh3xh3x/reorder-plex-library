#!/usr/bin/env python3

import os
import sqlite3
import argparse
import logging
from datetime import datetime
from shutil import copy2
from typing import Optional, Tuple, List, Dict, Any
from pathlib import Path
from contextlib import contextmanager
import sys
import csv
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PlexPathMapper:
    """Handle path mapping operations for Plex database paths."""
    
    def __init__(self, path_mappings: Optional[List[Tuple[str, str]]] = None, config_path: str = "path_mappings.conf"):
        """
        Initialize the path mapper.
        
        Args:
            path_mappings: List of (old_prefix, new_prefix) tuples
            config_path: Path to config file for path mappings
        """
        if path_mappings is not None:
            self.path_mappings = path_mappings
        else:
            self.path_mappings = self._load_mappings_from_file(config_path)
            if not self.path_mappings:
                raise ValueError("No path mappings found in config file and none provided")
    
    def _load_mappings_from_file(self, config_path: str) -> List[Tuple[str, str]]:
        mappings = []
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):  # skip comments/empty
                        continue
                    parts = line.split(",", 1)
                    if len(parts) == 2:
                        mappings.append( (parts[0].strip(), parts[1].strip()) )
        return mappings
    
    def map_path(self, plex_path: str) -> str:
        """
        Map Plex database paths to actual filesystem paths.
        
        Args:
            plex_path: Path as stored in Plex database
            
        Returns:
            Corrected path that should exist on filesystem
        """
        if not plex_path:
            return plex_path
        
        # Clean the path
        plex_path = plex_path.strip()
        
        # Apply path mappings
        for old_prefix, new_prefix in self.path_mappings:
            if plex_path.startswith(old_prefix):
                return plex_path.replace(old_prefix, new_prefix, 1)
        
        return plex_path
    
    def get_file_info(self, plex_path: str) -> Tuple[Optional[datetime], Optional[str], bool]:
        """
        Get file modification time, trying path mapping if original path fails.
        
        Args:
            plex_path: Original path from Plex database
            
        Returns:
            Tuple of (datetime_mtime, actual_path_used, exists)
        """
        if not plex_path:
            return None, None, False
        
        # Try original path first
        original_path = Path(plex_path)
        if original_path.is_file():
            try:
                mtime = datetime.fromtimestamp(original_path.stat().st_mtime)
                return mtime, str(original_path), True
            except (OSError, ValueError) as e:
                logger.warning(f"Error reading mtime for {plex_path}: {e}")
        
        # Try mapped path
        mapped_path = self.map_path(plex_path)
        if mapped_path != plex_path:
            mapped_path_obj = Path(mapped_path)
            if mapped_path_obj.is_file():
                try:
                    mtime = datetime.fromtimestamp(mapped_path_obj.stat().st_mtime)
                    return mtime, mapped_path, True
                except (OSError, ValueError) as e:
                    logger.warning(f"Error reading mtime for mapped path {mapped_path}: {e}")
        
        return None, None, False


class CSVHandler:
    """Handle CSV export and import operations."""
    
    @staticmethod
    def export_full_media_data_to_csv(db_path: str, csv_path: str, path_mapper: PlexPathMapper,
                                     limit: Optional[int] = None) -> bool:
        """
        Export complete media data with file modification times to CSV.
        This exports ALL data needed to recreate entries with updated timestamps.
        
        Args:
            db_path: Path to Plex database
            csv_path: Path to output CSV file
            path_mapper: PlexPathMapper instance for path resolution
            limit: Optional limit on number of records
            
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Query to get all necessary data
            query = """
                SELECT 
                    metadata_items.id,
                    metadata_items.metadata_type,
                    metadata_items.media_item_count,
                    metadata_items.title,
                    metadata_items.title_sort,
                    metadata_items.original_title,
                    metadata_items.studio,
                    metadata_items.rating,
                    metadata_items.rating_count,
                    metadata_items.tagline,
                    metadata_items.summary,
                    metadata_items.content_rating,
                    metadata_items.duration,
                    metadata_items.user_thumb_url,
                    metadata_items.user_art_url,
                    metadata_items.user_banner_url,
                    metadata_items.user_music_url,
                    metadata_items.tags_genre,
                    metadata_items.tags_director,
                    metadata_items.tags_writer,
                    metadata_items.tags_star,
                    metadata_items.originally_available_at,
                    metadata_items.available_at,
                    metadata_items.added_at,
                    metadata_items.created_at,
                    metadata_items.updated_at,
                    metadata_items.library_section_id,
                    metadata_items.parent_id,
                    metadata_items.hash,
                    media_items.id as media_item_id,
                    media_items.size,
                    media_items.width,
                    media_items.height,
                    media_items.container,
                    media_items.video_codec,
                    media_items.audio_codec,
                    media_parts.id as media_part_id,
                    media_parts.file as original_file_path,
                    media_parts.size as part_size,
                    media_parts.duration as part_duration,
                    media_parts.created_at as part_created_at,
                    media_parts.updated_at as part_updated_at
                FROM metadata_items
                JOIN media_items ON metadata_items.id = media_items.metadata_item_id
                JOIN media_parts ON media_items.id = media_parts.media_item_id
                WHERE media_parts.file IS NOT NULL 
                AND LENGTH(media_parts.file) > 0
                ORDER BY metadata_items.updated_at DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            columns = [description[0] for description in cursor.description]
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                # Add custom columns for our processing
                custom_columns = [
                    'file_exists', 'actual_file_path', 'file_mtime', 
                    'file_mtime_timestamp', 'path_was_mapped', 'new_updated_at'
                ]
                all_columns = columns + custom_columns
                
                writer = csv.DictWriter(csvfile, fieldnames=all_columns)
                writer.writeheader()
                
                rows_processed = 0
                files_found = 0
                
                for row in cursor:
                    row_dict = dict(zip(columns, row))
                    
                    # Get file modification time using path mapping
                    original_path = row_dict['original_file_path']
                    file_mtime, actual_path, exists = path_mapper.get_file_info(original_path)
                    
                    # Add custom fields
                    row_dict['file_exists'] = 'true' if exists else 'false'
                    row_dict['actual_file_path'] = actual_path or ''
                    row_dict['file_mtime'] = file_mtime.isoformat() if file_mtime else ''
                    row_dict['file_mtime_timestamp'] = int(file_mtime.timestamp()) if file_mtime else ''
                    row_dict['path_was_mapped'] = 'true' if actual_path != original_path else 'false'
                    
                    # Set new_updated_at to file mtime if file exists, otherwise keep original
                    if exists and file_mtime:
                        row_dict['new_updated_at'] = int(file_mtime.timestamp())
                        files_found += 1
                    else:
                        row_dict['new_updated_at'] = row_dict['updated_at']
                    
                    writer.writerow(row_dict)
                    rows_processed += 1
                    
                    if rows_processed % 100 == 0:
                        logger.info(f"Processed {rows_processed} records...")
            
            conn.close()
            
            logger.info(f"Successfully exported {rows_processed} records to {csv_path}")
            logger.info(f"Files found: {files_found}, Files missing: {rows_processed - files_found}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            return False
    
    @staticmethod
    def create_db_from_csv(csv_path: str, template_db_path: str, output_db_path: str,
                          fix_invalid_dates: bool = True) -> bool:
        """
        Create a new database from CSV data using a template database.
        
        Args:
            csv_path: Path to CSV file with media data
            template_db_path: Path to template/original Plex database
            output_db_path: Path for output database
            fix_invalid_dates: Whether to fix invalid timestamps
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # First copy the template database to preserve schema and other tables
            logger.info(f"Creating database from template: {template_db_path}")
            copy2(template_db_path, output_db_path)
            
            # Read CSV data
            updates = {}
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    if row.get('file_exists') == 'true' and row.get('new_updated_at'):
                        metadata_id = int(row['id'])
                        new_timestamp = int(row['new_updated_at'])
                        
                        # Validate timestamp if requested
                        if fix_invalid_dates:
                            current_year = datetime.now().year
                            try:
                                dt = datetime.fromtimestamp(new_timestamp)
                                if dt.year > current_year + 1 or dt.year < 1970:
                                    logger.warning(f"Skipping invalid timestamp for item {metadata_id}: {dt}")
                                    continue
                            except (ValueError, OSError):
                                logger.warning(f"Invalid timestamp for item {metadata_id}: {new_timestamp}")
                                continue
                        
                        updates[metadata_id] = new_timestamp
            
            if not updates:
                logger.error("No valid updates found in CSV")
                return False
            
            # Apply updates to the database
            conn = sqlite3.connect(output_db_path, check_same_thread=False)
            try:
                # Use same pragmas as before for compatibility
                conn.execute("PRAGMA ignore_check_constraints = ON")
                conn.execute("PRAGMA synchronous = OFF")
                conn.execute("PRAGMA journal_mode = MEMORY")
                
                cursor = conn.cursor()
                
                successful_updates = 0
                for metadata_id, new_timestamp in updates.items():
                    try:
                        cursor.execute(
                            "UPDATE metadata_items SET updated_at = ? WHERE id = ?",
                            (new_timestamp, metadata_id)
                        )
                        successful_updates += 1
                    except sqlite3.Error as e:
                        logger.warning(f"Failed to update item {metadata_id}: {e}")
                
                conn.commit()
                logger.info(f"Successfully updated {successful_updates} entries in {output_db_path}")
                
                return successful_updates > 0
                
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Error creating database from CSV: {e}")
            return False
    
    @staticmethod
    def analyze_csv(csv_path: str) -> None:
        """
        Analyze and display summary of CSV data.
        
        Args:
            csv_path: Path to CSV file
        """
        try:
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                total_records = 0
                files_exist = 0
                paths_mapped = 0
                updates_pending = 0
                libraries = {}
                
                for row in reader:
                    total_records += 1
                    
                    if row.get('file_exists') == 'true':
                        files_exist += 1
                    
                    if row.get('path_was_mapped') == 'true':
                        paths_mapped += 1
                    
                    if row.get('updated_at') != row.get('new_updated_at'):
                        updates_pending += 1
                    
                    lib_id = row.get('library_section_id', 'Unknown')
                    libraries[lib_id] = libraries.get(lib_id, 0) + 1
                
                print(f"\nCSV Analysis for: {csv_path}")
                print("=" * 60)
                print(f"Total records: {total_records}")
                print(f"Files found: {files_exist} ({files_exist/total_records*100:.1f}%)")
                print(f"Files missing: {total_records - files_exist} ({(total_records - files_exist)/total_records*100:.1f}%)")
                print(f"Paths mapped: {paths_mapped}")
                print(f"Updates pending: {updates_pending}")
                print(f"\nRecords by library:")
                for lib_id, count in sorted(libraries.items()):
                    print(f"  Library {lib_id}: {count} records")
                    
        except Exception as e:
            logger.error(f"Error analyzing CSV: {e}")


class PlexDatabaseManager:
    """Manage Plex database operations."""
    
    def __init__(self, db_path: str, path_mapper: PlexPathMapper):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the Plex database
            path_mapper: PlexPathMapper instance
        """
        self.db_path = Path(db_path)
        self.path_mapper = path_mapper
        
        if not self.db_path.is_file():
            raise FileNotFoundError(f"Database not found at: {db_path}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(str(self.db_path))
        try:
            yield conn
        finally:
            conn.close()
    
    def analyze_path_mappings(self, limit: int = 10) -> Dict[str, int]:
        """
        Analyze which path mappings are needed.
        
        Args:
            limit: Number of paths to analyze
            
        Returns:
            Dictionary with analysis results
        """
        logger.info("=== PATH MAPPING ANALYSIS ===")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            sql = ""
            if limit > 0:
                cursor.execute(
                    "SELECT file FROM media_parts WHERE file IS NOT NULL AND LENGTH(file) > 0 LIMIT ?;",
                    (limit,)
                )
            else:
                cursor.execute(
                    "SELECT file FROM media_parts WHERE file IS NOT NULL AND LENGTH(file) > 0;"
                )
            paths = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Analyzing {len(paths)} file paths...")
        
        results = {
            'original_exists': 0,
            'mapped_exists': 0,
            'still_missing': 0,
            'total': len(paths)
        }
        
        for plex_path in paths:
            original_exists = Path(plex_path).exists()
            mapped_path = self.path_mapper.map_path(plex_path)
            mapped_exists = Path(mapped_path).exists() if mapped_path else False
            
            if original_exists:
                results['original_exists'] += 1
                status = "✓ ORIGINAL"
            elif mapped_exists:
                results['mapped_exists'] += 1
                status = "✓ MAPPED"
            else:
                results['still_missing'] += 1
                status = "✗ MISSING"
            
            logger.info(f"{status} | {plex_path}")
            if mapped_path != plex_path:
                logger.info(f"      -> {mapped_path}")
        
        success_rate = ((results['original_exists'] + results['mapped_exists']) / results['total'] * 100) if results['total'] > 0 else 0.0
        results['success_rate'] = success_rate
        
        logger.info(f"\nResults:")
        logger.info(f"  Original paths work: {results['original_exists']}")
        logger.info(f"  Mapped paths work: {results['mapped_exists']}")
        logger.info(f"  Still missing: {results['still_missing']}")
        logger.info(f"  Success rate: {success_rate:.1f}%")
        
        return results
    
    def get_recent_media(self, limit: int = 30, sort_by: str = 'file_mtime') -> Tuple[List[Dict], List[Tuple]]:
        """
        Get recent media with path mapping correction.
        
        Args:
            limit: Maximum number of entries to return
            sort_by: Field to sort by
            
        Returns:
            Tuple of (entries, updates)
        """
        entries = []
        updates = []
        
        query = """
            SELECT 
                metadata_items.id,
                metadata_items.title, 
                metadata_items.added_at,
                metadata_items.created_at,
                metadata_items.updated_at,
                metadata_items.library_section_id,
                media_parts.file as file_path
            FROM metadata_items
            JOIN media_items ON metadata_items.id = media_items.metadata_item_id
            JOIN media_parts ON media_items.id = media_parts.media_item_id
            WHERE metadata_items.title IS NOT NULL
            AND media_parts.file IS NOT NULL 
            AND LENGTH(media_parts.file) > 0
            ORDER BY metadata_items.updated_at DESC;
        """
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
        
        logger.info(f"Processing {len(rows)} media files with path mapping...")
        
        stats = {'files_found': 0, 'files_mapped': 0, 'files_missing': 0}
        
        for item_id, title, added_at, created_at, updated_at, library_section_id, file_path in rows:
            entry = self._create_entry(
                item_id, title, added_at, created_at, updated_at,
                library_section_id, file_path
            )
            
            # Get file modification time with path mapping
            file_mtime, actual_path, exists = self.path_mapper.get_file_info(file_path)
            
            if exists:
                entry["file_mtime"] = file_mtime
                entry["actual_path"] = actual_path
                entry["file_exists"] = True
                entry["path_mapped"] = (actual_path != file_path)
                
                if actual_path == file_path:
                    stats['files_found'] += 1
                else:
                    stats['files_mapped'] += 1
                
                updates.append((int(file_mtime.timestamp()), item_id))
            else:
                stats['files_missing'] += 1
            
            entries.append(entry)
        
        logger.info(
            f"Results: {stats['files_found']} original, "
            f"{stats['files_mapped']} mapped, {stats['files_missing']} missing"
        )
        
        # Sort entries
        entries = self._sort_entries(entries, sort_by)
        entries = entries[:limit]
        
        # Filter updates to match limited entries
        limited_item_ids = {entry['item_id'] for entry in entries}
        updates = [(mtime, item_id) for mtime, item_id in updates if item_id in limited_item_ids]
        
        return entries, updates
    
    def _create_entry(self, item_id: int, title: str, added_at: Any, created_at: Any,
                      updated_at: Any, library_section_id: int, file_path: str) -> Dict:
        """Create a media entry dictionary."""
        entry = {
            "item_id": item_id,
            "title": title,
            "library_section_id": library_section_id,
            "added_at": None,
            "created_at": None,
            "updated_at": None,
            "plex_path": file_path,
            "actual_path": None,
            "file_mtime": None,
            "file_exists": False,
            "path_mapped": False
        }
        
        # Convert Plex timestamps
        for field_name, timestamp in [
            ('added_at', added_at),
            ('created_at', created_at),
            ('updated_at', updated_at)
        ]:
            if isinstance(timestamp, (int, float)) and timestamp > 0:
                entry[field_name] = datetime.fromtimestamp(timestamp)
        
        return entry
    
    def _sort_entries(self, entries: List[Dict], sort_by: str) -> List[Dict]:
        """Sort entries by specified field."""
        if sort_by == 'file_mtime':
            return sorted(entries, key=lambda x: x['file_mtime'] or datetime.min, reverse=True)
        elif sort_by in ['added_at', 'created_at', 'updated_at']:
            return sorted(entries, key=lambda x: x[sort_by] or datetime.min, reverse=True)
        else:
            return entries
    
    def update_database_copy(self, updates: List[Tuple], output_path: str,
                           fix_invalid_dates: bool = True) -> bool:
        """
        Create a copy of the database with updated timestamps.
        
        Args:
            updates: List of (timestamp, item_id) tuples
            output_path: Path for the output database
            fix_invalid_dates: Whether to fix invalid timestamps
            
        Returns:
            True if successful, False otherwise
        """
        import subprocess
        
        try:
            # Common locations for Plex SQLite binary
            home_dir = os.path.expanduser("~")
            plex_sqlite_paths = [
                # User installations
                os.path.join(home_dir, "Plex Media Server", "Plex SQLite"),
                os.path.join(home_dir, "plexmediaserver", "Plex SQLite"),
                # System installations
                "/usr/lib/plexmediaserver/Plex SQLite",
                "/opt/plexmediaserver/Plex SQLite",
                "/usr/local/plexmediaserver/Plex SQLite",
                "/var/packages/PlexMediaServer/target/Plex SQLite",  # Synology
                "/usr/pbi/plexmediaserver-amd64/share/plexmediaserver/Plex SQLite",  # FreeBSD
                "/usr/local/share/plexmediaserver/Plex SQLite",  # Some Linux distros
                # Snap installation
                "/snap/plexmediaserver/current/Plex SQLite",
                # Docker common paths
                "/app/Plex SQLite",
            ]
            
            # Find Plex SQLite binary
            plex_sqlite = None
            for path in plex_sqlite_paths:
                if os.path.exists(path):
                    plex_sqlite = path
                    logger.info(f"Found Plex SQLite at: {plex_sqlite}")
                    break
            
            # Try to find it using which/whereis
            if not plex_sqlite:
                try:
                    result = subprocess.run(['which', 'Plex SQLite'], capture_output=True, text=True)
                    if result.returncode == 0 and result.stdout.strip():
                        plex_sqlite = result.stdout.strip()
                        logger.info(f"Found Plex SQLite via which: {plex_sqlite}")
                except (subprocess.CalledProcessError, FileNotFoundError):
                    pass
            
            logger.info(f"Creating database copy at {output_path}")
            
            # Use Plex SQLite if available
            if plex_sqlite and os.path.exists(plex_sqlite):
                logger.info("Using Plex SQLite for database copy")
                # Escape paths for shell
                escaped_db = str(self.db_path).replace("'", "'\"'\"'")
                escaped_output = output_path.replace("'", "'\"'\"'")
                
                # Use .backup command
                cmd = [plex_sqlite, escaped_db, f".backup '{escaped_output}'"]
                result = subprocess.run(' '.join(cmd), shell=True, capture_output=True, text=True)
                
                if result.returncode != 0:
                    logger.error(f"Plex SQLite backup failed: {result.stderr}")
                    # Fall back to file copy
                    copy2(str(self.db_path), output_path)
            else:
                # Direct file copy
                logger.info("Using direct file copy (Plex SQLite not found)")
                copy2(str(self.db_path), output_path)
            
            # Apply updates - try to connect with check_same_thread=False for Plex compatibility
            conn = sqlite3.connect(output_path, check_same_thread=False)
            try:
                # Disable FTS tokenizer checks that might cause issues
                conn.execute("PRAGMA ignore_check_constraints = ON")
                conn.execute("PRAGMA synchronous = OFF")
                conn.execute("PRAGMA journal_mode = MEMORY")
                
                cursor = conn.cursor()
                
                successful_updates = 0
                for mtime, item_id in updates:
                    # Validate timestamp if requested
                    if fix_invalid_dates:
                        current_year = datetime.now().year
                        mtime_dt = datetime.fromtimestamp(mtime)
                        
                        if mtime_dt.year > current_year + 1 or mtime_dt.year < 1970:
                            logger.warning(
                                f"Skipping invalid timestamp for item {item_id}: {mtime_dt}"
                            )
                            continue
                    
                    try:
                        cursor.execute(
                            "UPDATE metadata_items SET updated_at = ? WHERE id = ?",
                            (mtime, item_id)
                        )
                        successful_updates += 1
                    except sqlite3.Error as e:
                        logger.warning(f"Failed to update item {item_id}: {e}")
                
                conn.commit()
                logger.info(f"Successfully updated {successful_updates} entries")
                
                return successful_updates > 0
                
            finally:
                conn.close()
            
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            logger.info("This may be due to Plex database custom extensions.")
            logger.info("Try using --use-plex-sqlite flag for instructions.")
            return False


class ResultDisplay:
    """Handle result display formatting."""
    
    @staticmethod
    def display_entries(entries: List[Dict]) -> None:
        """Display entries in a formatted table."""
        if not entries:
            print("No entries found.")
            return
        
        # Count invalid timestamps
        invalid_timestamps = 0
        current_year = datetime.now().year
        
        for entry in entries:
            file_mtime = entry.get('file_mtime')
            if file_mtime and (file_mtime.year > current_year + 1 or file_mtime.year < 1970):
                invalid_timestamps += 1
        
        print(f"\nTop {len(entries)} Media Items (with path mapping):")
        if invalid_timestamps > 0:
            print(f"⚠️  Found {invalid_timestamps} files with invalid timestamps (future/past dates)")
        
        print("=" * 140)
        print(f"{'Title':<30} {'Lib':<3} {'File Modified':<20} {'Mapped':<6} {'Status':<8} {'File':<25}")
        print("-" * 140)
        
        for entry in entries:
            ResultDisplay._display_entry(entry, current_year)
    
    @staticmethod
    def _display_entry(entry: Dict, current_year: int) -> None:
        """Display a single entry."""
        title = entry.get('title', 'N/A')[:29]
        lib_id = str(entry.get('library_section_id', 'N/A'))
        
        file_mtime = entry.get('file_mtime')
        if file_mtime:
            # Mark invalid timestamps
            if file_mtime.year > current_year + 1 or file_mtime.year < 1970:
                file_mtime_str = f"{file_mtime.strftime('%Y-%m-%d %H:%M:%S')} ⚠️"
            else:
                file_mtime_str = file_mtime.strftime('%Y-%m-%d %H:%M:%S')
        else:
            file_mtime_str = 'N/A'
        
        mapped = 'Yes' if entry.get('path_mapped') else 'No'
        exists = 'Found' if entry.get('file_exists') else 'Missing'
        
        actual_path = entry.get('actual_path') or entry.get('plex_path', '')
        filename = os.path.basename(actual_path) if actual_path else 'N/A'
        filename = filename[:24] if len(filename) > 24 else filename
        
        print(f"{title:<30} {lib_id:<3} {file_mtime_str:<20} {mapped:<6} {exists:<8} {filename:<25}")


def main():
    parser = argparse.ArgumentParser(description="Plex tool with path mapping correction.")
    parser.add_argument("--db", type=str, 
                       default=os.path.expanduser("~/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db"),
                       help="Path to Plex database file.")
    parser.add_argument("--write", action="store_true", help="Write modified copy of DB with corrected file times.")
    parser.add_argument("--output", type=str, default=os.path.expanduser("~/plex_fixed_dates.db"),
                       help="Output path for modified database.")
    parser.add_argument("--export-csv", type=str, metavar="PATH",
                       help="Export complete media data to CSV file.")
    parser.add_argument("--import-csv", type=str, metavar="PATH",
                       help="Create database from CSV file (requires --output).")
    parser.add_argument("--analyze-csv", type=str, metavar="PATH",
                       help="Analyze and display summary of CSV file.")
    parser.add_argument("--fix-invalid-dates", action="store_true", default=True,
                       help="Fix invalid future/past timestamps (default: enabled).")
    parser.add_argument("--keep-invalid-dates", dest="fix_invalid_dates", action="store_false",
                       help="Keep invalid timestamps as-is.")
    parser.add_argument("--analyze", action="store_true", help="Analyze path mapping needs.")
    parser.add_argument("--limit", type=int, default=30, help="Number of entries to process.")
    parser.add_argument("--sort-by", choices=['file_mtime', 'added_at', 'created_at', 'updated_at'],
                       default='file_mtime', help="Sort by this field.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging.")
    parser.add_argument("--use-plex-sqlite", action="store_true", 
                       help="Use Plex's SQLite binary if available (recommended for --write).")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Handle CSV analysis mode
    if args.analyze_csv:
        CSVHandler.analyze_csv(args.analyze_csv)
        return 0
    
    # Handle CSV import mode
    if args.import_csv:
        if not args.output:
            logger.error("--import-csv requires --output to specify the output database path")
            return 1
        
        logger.info(f"Creating database from CSV: {args.import_csv}")
        success = CSVHandler.create_db_from_csv(
            args.import_csv, args.db, args.output, args.fix_invalid_dates
        )
        return 0 if success else 1
    
    # Handle CSV export mode
    if args.export_csv:
        logger.info(f"Exporting complete media data to CSV: {args.export_csv}")
        path_mapper = PlexPathMapper()
        success = CSVHandler.export_full_media_data_to_csv(
            args.db, args.export_csv, path_mapper, args.limit
        )
        if success:
            print(f"\nExported media data to: {args.export_csv}")
            print("\nYou can:")
            print(f"1. Analyze the CSV: python3 {sys.argv[0]} --analyze-csv {args.export_csv}")
            print(f"2. Create a new DB: python3 {sys.argv[0]} --import-csv {args.export_csv} --output <output_db>")
        return 0 if success else 1
    
    try:
        # Initialize components
        path_mapper = PlexPathMapper()
        db_manager = PlexDatabaseManager(args.db, path_mapper)
        
        if args.analyze:
            db_manager.analyze_path_mappings(args.limit)
        else:
            entries, updates = db_manager.get_recent_media(args.limit, args.sort_by)
            ResultDisplay.display_entries(entries)
            
            # Filter valid updates
            valid_updates = [
                (mtime, item_id) for mtime, item_id in updates 
                if any(e['item_id'] == item_id and e['file_exists'] for e in entries)
            ]
            
            if not valid_updates:
                print("No valid file updates to apply.")
                return 1
            
            print(f"\nFound {len(valid_updates)} files with valid modification times.")
            
            # Write to database if requested
            if args.write:
                print(f"\nPreparing to update {len(valid_updates)} files with real modification times...")
                
                if args.use_plex_sqlite:
                    print("\nNote: Using Plex SQLite requires the Plex Media Server SQLite binary.")
                    print("You may need to run commands like:")
                    print(f'  "~/Plex Media Server/Plex SQLite" "{args.db}" ".backup {args.output}"')
                    print("Then apply updates using the Plex SQLite binary.")
                    return 0
                
                success = db_manager.update_database_copy(
                    valid_updates, args.output, args.fix_invalid_dates
                )
                return 0 if success else 1
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


# ======================== TESTS ========================

import unittest
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock


class TestPlexPathMapper(unittest.TestCase):
    """Test cases for PlexPathMapper class."""
    
    def setUp(self):
        # Create a temporary config file for path mappings
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.temp_dir, "path_mappings.conf")
        with open(self.config_path, "w", encoding="utf-8") as f:
            f.write("/unittest/,/home/unittest/\n")
        self.mapper = PlexPathMapper(config_path=self.config_path)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_map_path_with_mapping(self):
        """Test path mapping with matching prefix."""
        result = self.mapper.map_path("/unittest/movies/movie.mkv")
        self.assertEqual(result, "/home/unittest/movies/movie.mkv")
    
    def test_map_path_without_mapping(self):
        """Test path mapping without matching prefix."""
        result = self.mapper.map_path("/other/path/file.mkv")
        self.assertEqual(result, "/other/path/file.mkv")
    
    def test_map_path_empty(self):
        """Test path mapping with empty string."""
        result = self.mapper.map_path("")
        self.assertEqual(result, "")
    
    def test_map_path_none(self):
        """Test path mapping with None."""
        result = self.mapper.map_path(None)
        self.assertIsNone(result)
    
    def test_custom_mappings(self):
        """Test with custom path mappings via config file."""
        # Write a new config file with a different mapping
        with open(self.config_path, "w", encoding="utf-8") as f:
            f.write("/old/,/new/\n")
        custom_mapper = PlexPathMapper(config_path=self.config_path)
        result = custom_mapper.map_path("/old/file.txt")
        self.assertEqual(result, "/new/file.txt")
    
    @patch('pathlib.Path.is_file')
    @patch('pathlib.Path.stat')
    def test_get_file_info_original_exists(self, mock_stat, mock_is_file):
        """Test get_file_info when original path exists."""
        mock_is_file.return_value = True
        mock_stat.return_value = Mock(st_mtime=1234567890)
        
        mtime, path, exists = self.mapper.get_file_info("/test/file.mkv")
        
        self.assertIsNotNone(mtime)
        self.assertEqual(path, "/test/file.mkv")
        self.assertTrue(exists)
    
    @patch('pathlib.Path.is_file')
    @patch('pathlib.Path.stat')
    def test_get_file_info_mapped_exists(self, mock_stat, mock_is_file):
        """Test get_file_info when only mapped path exists."""
        # First call for original path returns False, second for mapped returns True
        mock_is_file.side_effect = [False, True]
        mock_stat.return_value = Mock(st_mtime=1234567890)
        
        mtime, path, exists = self.mapper.get_file_info("/unittest/file.mkv")
        
        self.assertIsNotNone(mtime)
        self.assertEqual(path, "/home/unittest/file.mkv")
        self.assertTrue(exists)
    
    @patch('pathlib.Path.is_file')
    def test_get_file_info_not_exists(self, mock_is_file):
        """Test get_file_info when file doesn't exist."""
        mock_is_file.return_value = False
        
        mtime, path, exists = self.mapper.get_file_info("/nonexistent/file.mkv")
        
        self.assertIsNone(mtime)
        self.assertIsNone(path)
        self.assertFalse(exists)


class TestCSVHandler(unittest.TestCase):
    """Test cases for CSVHandler class."""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.csv_path = os.path.join(self.temp_dir, "test.csv")
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_analyze_csv(self):
        """Test CSV analysis functionality."""
        # Create test CSV
        with open(self.csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'id', 'title', 'file_exists', 'path_was_mapped', 
                'updated_at', 'new_updated_at', 'library_section_id'
            ])
            writer.writeheader()
            writer.writerow({
                'id': '1', 'title': 'Test Movie',
                'file_exists': 'true', 'path_was_mapped': 'true',
                'updated_at': '1234567890', 'new_updated_at': '1234567891',
                'library_section_id': '1'
            })
            writer.writerow({
                'id': '2', 'title': 'Test Movie 2',
                'file_exists': 'false', 'path_was_mapped': 'false',
                'updated_at': '1234567890', 'new_updated_at': '1234567890',
                'library_section_id': '1'
            })
        
        # Capture output
        with patch('builtins.print') as mock_print:
            CSVHandler.analyze_csv(self.csv_path)
            
        # Verify analysis was performed
        calls = [str(call) for call in mock_print.call_args_list]
        self.assertTrue(any("Total records: 2" in call for call in calls))
        self.assertTrue(any("Files found: 1" in call for call in calls))
    
    def test_create_db_from_csv_no_updates(self):
        """Test creating database from CSV with no valid updates."""
        # Create empty CSV
        with open(self.csv_path, 'w') as f:
            f.write("id,file_exists,new_updated_at\n")
        
        # Create dummy template db
        template_db = os.path.join(self.temp_dir, "template.db")
        output_db = os.path.join(self.temp_dir, "output.db")
        
        # Create minimal template database
        conn = sqlite3.connect(template_db)
        conn.execute("CREATE TABLE metadata_items (id INTEGER PRIMARY KEY, updated_at INTEGER)")
        conn.close()
        
        result = CSVHandler.create_db_from_csv(self.csv_path, template_db, output_db)
        
        self.assertFalse(result)


class TestPlexDatabaseManager(unittest.TestCase):
    """Test cases for PlexDatabaseManager class."""
    
    def setUp(self):
        # Create temporary database
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self._create_test_database()
        
        self.mapper = PlexPathMapper()
        self.db_manager = PlexDatabaseManager(self.db_path, self.mapper)
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def _create_test_database(self):
        """Create a minimal test database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create minimal schema
        cursor.execute("""
            CREATE TABLE metadata_items (
                id INTEGER PRIMARY KEY,
                title TEXT,
                added_at INTEGER,
                created_at INTEGER,
                updated_at INTEGER,
                library_section_id INTEGER
            )
        """)
        
        cursor.execute("""
            CREATE TABLE media_items (
                id INTEGER PRIMARY KEY,
                metadata_item_id INTEGER
            )
        """)
        
        cursor.execute("""
            CREATE TABLE media_parts (
                id INTEGER PRIMARY KEY,
                media_item_id INTEGER,
                file TEXT
            )
        """)
        
        # Insert test data
        cursor.execute(
            "INSERT INTO metadata_items VALUES (1, 'Test Movie', 1234567890, 1234567890, 1234567890, 1)"
        )
        cursor.execute("INSERT INTO media_items VALUES (1, 1)")
        cursor.execute("INSERT INTO media_parts VALUES (1, 1, '/unittest/movies/test.mkv')")
        
        conn.commit()
        conn.close()
    
    def test_get_connection(self):
        """Test database connection context manager."""
        with self.db_manager.get_connection() as conn:
            self.assertIsNotNone(conn)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM metadata_items")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1)
    
    def test_database_not_found(self):
        """Test initialization with non-existent database."""
        with self.assertRaises(FileNotFoundError):
            PlexDatabaseManager("/nonexistent/path.db", self.mapper)
    
    @patch.object(PlexPathMapper, 'get_file_info')
    def test_get_recent_media(self, mock_get_file_info):
        """Test getting recent media entries."""
        mock_get_file_info.return_value = (
            datetime.fromtimestamp(1234567890),
            "/home/unittest/movies/test.mkv",
            True
        )
        
        entries, updates = self.db_manager.get_recent_media(limit=10)
        
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]['title'], 'Test Movie')
        self.assertTrue(entries[0]['file_exists'])
        self.assertTrue(entries[0]['path_mapped'])
    
    def test_sort_entries(self):
        """Test entry sorting functionality."""
        entries = [
            {'file_mtime': datetime(2023, 1, 1), 'title': 'A'},
            {'file_mtime': datetime(2023, 1, 3), 'title': 'B'},
            {'file_mtime': datetime(2023, 1, 2), 'title': 'C'},
        ]
        
        sorted_entries = self.db_manager._sort_entries(entries, 'file_mtime')
        
        self.assertEqual(sorted_entries[0]['title'], 'B')
        self.assertEqual(sorted_entries[1]['title'], 'C')
        self.assertEqual(sorted_entries[2]['title'], 'A')
    
    def test_create_entry(self):
        """Test entry creation with timestamp conversion."""
        entry = self.db_manager._create_entry(
            1, "Test", 1234567890, 1234567890, 1234567890, 1, "/test/file.mkv"
        )
        
        self.assertEqual(entry['item_id'], 1)
        self.assertEqual(entry['title'], "Test")
        self.assertIsInstance(entry['added_at'], datetime)
        self.assertEqual(entry['added_at'].year, 2009)  # 1234567890 = Feb 2009

    def test_analyze_path_mappings_limit(self):
        """Test analyze_path_mappings with limit=0 and nonzero."""
        # Insert a second file for nonzero test
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO media_parts VALUES (?, ?, ?)", (2, 1, '/unittest/movies/another.mkv'))
        conn.commit()
        conn.close()

        # Test with limit=1 (should return 1 path)
        results = self.db_manager.analyze_path_mappings(limit=1)
        self.assertEqual(results['total'], 1)
        # Test with limit=0 (should return all paths, i.e., 2)
        results = self.db_manager.analyze_path_mappings(limit=0)
        self.assertEqual(results['total'], 2)


class TestResultDisplay(unittest.TestCase):
    """Test cases for ResultDisplay class."""
    
    @patch('builtins.print')
    def test_display_entries_empty(self, mock_print):
        """Test displaying empty entries."""
        ResultDisplay.display_entries([])
        mock_print.assert_called_with("No entries found.")
    
    @patch('builtins.print')
    def test_display_entries_with_data(self, mock_print):
        """Test displaying entries with data."""
        entries = [{
            'title': 'Test Movie',
            'library_section_id': 1,
            'file_mtime': datetime(2023, 1, 1, 12, 0, 0),
            'path_mapped': True,
            'file_exists': True,
            'actual_path': '/home/unittest/movies/test.mkv'
        }]
        
        ResultDisplay.display_entries(entries)
        
        # Check that header was printed
        calls = [str(call) for call in mock_print.call_args_list]
        self.assertTrue(any("Top 1 Media Items" in call for call in calls))
    
    @patch('builtins.print')
    def test_display_entries_with_invalid_timestamp(self, mock_print):
        """Test displaying entries with invalid timestamps."""
        entries = [{
            'title': 'Future Movie',
            'library_section_id': 1,
            'file_mtime': datetime(2050, 1, 1),  # Future date
            'path_mapped': False,
            'file_exists': True,
            'actual_path': '/test/movie.mkv'
        }]
        
        ResultDisplay.display_entries(entries)
        
        # Check that warning was printed
        calls = [str(call) for call in mock_print.call_args_list]
        self.assertTrue(any("Found 1 files with invalid timestamps" in call for call in calls))


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete tool."""
    
    def setUp(self):
        # Create a temporary test database
        self.temp_dir = tempfile.mkdtemp()
        self.test_db = os.path.join(self.temp_dir, "test.db")
        self._create_minimal_db()
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def _create_minimal_db(self):
        """Create a minimal test database."""
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE media_parts (file TEXT)")
        cursor.execute("INSERT INTO media_parts VALUES ('/test/file.mkv')")
        conn.commit()
        conn.close()
    
    @patch('sys.argv', ['plex_mapper.py', '--analyze', '--limit', '5'])
    def test_main_analyze_mode(self):
        """Test main function in analyze mode."""
        # Patch sys.argv to include our test database
        with patch('sys.argv', ['plex_mapper.py', '--analyze', '--limit', '5', '--db', self.test_db]):
            with patch('pathlib.Path.exists', return_value=True):
                result = main()
        
        self.assertEqual(result, 0)
    
    @patch('sys.argv', ['plex_mapper.py', '--db', '/nonexistent/db.db'])
    def test_main_database_not_found(self):
        """Test main function with non-existent database."""
        result = main()
        self.assertEqual(result, 1)


if __name__ == "__main__":
    # Check if we're running tests
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        # Run tests
        sys.argv = [sys.argv[0]]  # Reset argv for unittest
        unittest.main(verbosity=2)
    else:
        # Run main program
        exit(main())