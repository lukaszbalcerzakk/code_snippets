import zipfile
import json
import pandas as pd
import re
import os
import glob
import sys
from io import TextIOWrapper
from typing import List, Dict, Set, Any, Tuple, Union
from pathlib import Path
from datetime import datetime
from collections import OrderedDict


# 🚫 TABLES EXCLUDED FROM ANALYSIS AND COMMENTING OUT
TABLES_TO_EXCLUDE = [
    "RefreshDate",
]

# 🚫 EXCLUSION PATTERNS (case-insensitive)
EXCLUSION_PATTERNS = [
    "partition",  # PartitionMetadata, PartitionDate, partition_info, etc.
    "refresh",    # RefreshDate, LastRefresh, etc.
]

FIELDS_TO_EXCLUDE_FROM_MARTS_ANALYSIS = [
    'SourceSystem',
    'elt_dmr_core',
    'elt_dmr_marts'   
]

_commenting_error_log = []

# NOTE: The following configuration will be generated dynamically.
TABLES_AND_FIELDS = []

def log_and_print(message: str):
    global _analysis_log_details
    if '_analysis_log_details' not in globals():
        _analysis_log_details = []
    
    print(message)
    _analysis_log_details.append(message)



# ===========================
# 🚫 EXCLUSION FUNCTIONS
# ===========================

def is_table_excluded(table_name: str, tables_to_exclude: list = None, exclusion_patterns: list = None) -> Tuple[bool, str]:
    """
    Checks if a table should be excluded based on provided lists.
    """
    if tables_to_exclude is None:
        tables_to_exclude = TABLES_TO_EXCLUDE
    if exclusion_patterns is None:
        exclusion_patterns = EXCLUSION_PATTERNS
        
    name_lower = table_name.lower()
    
    for excluded in tables_to_exclude:
        if name_lower == excluded.lower():
            return True, f"exclusion_list:{excluded}"
    
    for pattern in exclusion_patterns:
        if pattern.lower() in name_lower:
            return True, f"pattern:{pattern}"
    
    return False, ""

def is_field_excluded(field_name: str, exclusion_patterns: list = None) -> Tuple[bool, str]:
    if exclusion_patterns is None:
        exclusion_patterns = EXCLUSION_PATTERNS
        
    name_lower = field_name.lower()
    
    for pattern in exclusion_patterns:
        if pattern.lower() in name_lower:
            return True, f"pattern:{pattern}"
    
    return False, ""


# ===========================
# 📁 HELPER FUNCTIONS
# ===========================

def find_key_recursively(element: Any, searched_key: str) -> Any:
    if isinstance(element, dict):
        if searched_key in element:
            return element[searched_key]
        for value in element.values():
            found = find_key_recursively(value, searched_key)
            if found is not None:
                return found
    elif isinstance(element, list):
        for item in element:
            found = find_key_recursively(item, searched_key)
            if found is not None:
                return found
    return None

def parse_alias_mapping(json_data: Any) -> Dict[str, str]:
    mapping = {}
    
    if not isinstance(json_data, dict):
        return mapping
    
    from_locations = []
    
    try:
        if 'prototypeQuery' in json_data and isinstance(json_data['prototypeQuery'], dict):
            proto_from = json_data['prototypeQuery'].get('From', [])
            if isinstance(proto_from, list):
                from_locations.append(proto_from)
    except (AttributeError, TypeError): pass
    
    try:
        if 'query' in json_data and isinstance(json_data['query'], dict):
            query_from = json_data['query'].get('From', [])
            if isinstance(query_from, list):
                from_locations.append(query_from)
    except (AttributeError, TypeError): pass
    
    try:
        recursive_from = find_key_recursively(json_data, 'From')
        if isinstance(recursive_from, list):
            from_locations.append(recursive_from)
    except (AttributeError, TypeError): pass
    
    for from_list in from_locations:
        if isinstance(from_list, list):
            for item in from_list:
                if isinstance(item, dict) and 'Name' in item and 'Entity' in item:
                    alias = item['Name']
                    table = item['Entity']
                    mapping[alias] = table
    
    return mapping

def extract_fields_from_where(where_item: Any, alias_mapping: Dict[str, str], all_fields: Dict[str, Dict]) -> List[str]:
    fields = []
    
    if isinstance(where_item, dict):
        source_ref = find_key_recursively(where_item, 'SourceRef')
        if isinstance(source_ref, dict):
            alias = source_ref.get('Source', '')
            for key in ['Property', 'Column']:
                property_name = where_item.get(key)
                if not property_name:
                    property_name = find_key_recursively(where_item, key)
                
                if alias in alias_mapping and property_name:
                    table_name = alias_mapping[alias]
                    full_field_name = f"{table_name}.{property_name}"
                    if full_field_name in all_fields:
                        fields.append(full_field_name)
    
    return fields

def extract_fields_from_measure(measure_item: Any, alias_mapping: Dict[str, str], all_fields: Dict[str, Dict]) -> List[str]:
    fields = []
    
    if isinstance(measure_item, dict):
        source_ref = find_key_recursively(measure_item, 'SourceRef')
        if isinstance(source_ref, dict):
            alias = source_ref.get('Source', '')
            property_name = find_key_recursively(measure_item, 'Property')
            
            if alias in alias_mapping and property_name:
                table_name = alias_mapping[alias]
                full_field_name = f"{table_name}.{property_name}"
                if full_field_name in all_fields:
                    fields.append(full_field_name)
    
    return fields

def find_fields_in_json_structure(json_data: Any, tables_and_fields: List[Dict]) -> List[Tuple[str, str, str]]:
    found_fields = []
    
    if not isinstance(json_data, dict):
        return found_fields
    
    try:
        alias_mapping = parse_alias_mapping(json_data)
        
        all_fields_dict = {}
        for tab_conf in tables_and_fields:
            table_name = tab_conf["table"]
            for field in tab_conf["fields"]:
                key = f"{table_name}.{field}"
                all_fields_dict[key] = {"table": table_name, "field": field}
        
        select_list = find_key_recursively(json_data, 'Select')
        if isinstance(select_list, list):
            for select_item in select_list:
                if isinstance(select_item, dict):
                    full_name = select_item.get('Name', '')
                    if full_name in all_fields_dict:
                        found_fields.append((full_name, 'VISUALIZATION', 'Select.Name'))
                    
                    column = select_item.get('Column', {})
                    if isinstance(column, dict):
                        expression = column.get('Expression', {})
                        if isinstance(expression, dict):
                            source_ref = expression.get('SourceRef', {})
                            if isinstance(source_ref, dict):
                                alias = source_ref.get('Source', '')
                                property_name = column.get('Property', '')
                                
                                if alias in alias_mapping and property_name:
                                    table_name = alias_mapping[alias]
                                    full_field_name = f"{table_name}.{property_name}"
                                    if full_field_name in all_fields_dict:
                                        found_fields.append((full_field_name, 'VISUALIZATION', f'SourceRef:{alias}→{table_name}'))
        
        where_list = find_key_recursively(json_data, 'Where')
        if isinstance(where_list, list):
            for where_item in where_list:
                fields_from_filters = extract_fields_from_where(where_item, alias_mapping, all_fields_dict)
                for field in fields_from_filters:
                    found_fields.append((field, 'FILTER', 'Where'))
        
        for measure_key in ['Measures', 'measures', 'Aggregates', 'aggregates']:
            measure_list = find_key_recursively(json_data, measure_key)
            if isinstance(measure_list, list):
                for measure_item in measure_list:
                    fields_from_measures = extract_fields_from_measure(measure_item, alias_mapping, all_fields_dict)
                    for field in fields_from_measures:
                        found_fields.append((field, 'MEASURE', measure_key))
    
    except Exception as e:
        pass
    
    return found_fields

# ===========================
# 🚀 DYNAMIC CONFIGURATION LOADING FUNCTION
# ===========================

def dynamically_generate_field_config(model_path: str, tables_to_exclude: list, exclusion_patterns: list, measures_folder_name: str) -> List[Dict]:
    """
    Dynamically generates the configuration of tables and fields from the model directory.
    Now accepts configuration as parameters instead of using globals.
    """
    final_config = []
    log_and_print(f"   Trying to find the main folder with tables...")
    
    folder_tables = ""
    for folder in os.listdir(model_path):
        if folder.lower() == "tables":
            folder_tables = os.path.join(model_path, folder)
            break
            
    if not folder_tables:
        log_and_print(f"   ❌ ERROR: 'tables' folder not found in the model directory: {model_path}")
        return []
    
    log_and_print(f"   📂 Found folder with tables: {folder_tables}")

    table_folders = [f.name for f in os.scandir(folder_tables) if f.is_dir()]
    log_and_print(f"   🔍 Found {len(table_folders)} potential folders with tables.")

    for table_name in table_folders:
        is_excluded, reason = is_table_excluded(table_name, tables_to_exclude, exclusion_patterns)
        if is_excluded:
            log_and_print(f"   ⤴ Skipping excluded table: '{table_name}' (reason: {reason})")
            continue

        if measures_folder_name in table_name.lower().strip('#'):
            log_and_print(f"   ⤴ Skipping measures folder: '{table_name}'")
            continue

        if 'calculat' in table_name.lower() or 'calc' in table_name.lower():
            log_and_print(f"   ⤴ Skipping calculated table: '{table_name}'")
            continue

        table_file_path = os.path.join(folder_tables, table_name, f"{table_name}.json")
        
        if not os.path.exists(table_file_path):
            log_and_print(f"      ⚠️ WARNING: Definition file '{os.path.basename(table_file_path)}' not found. Skipping folder.")
            continue
        
        try:
            with open(table_file_path, 'r', encoding='utf-8-sig') as f:
                table_data = json.load(f)
            
            json_column_list = table_data.get("columns", [])
            
            if json_column_list:
                calculated_columns = sum(1 for col in json_column_list if col.get("type") == "calculatedTableColumn")
                if calculated_columns > 0:
                    continue
            
            columns = []
            measures_in_table = []
            fields_in_hierarchies = set()
            
            for column in json_column_list:
                if "name" in column:
                    if column.get("type") == "calculated":
                        measures_in_table.append(column["name"])
                    else:
                        columns.append(column["name"])
            
            for hierarchy in table_data.get("hierarchies", []):
                for level in hierarchy.get("levels", []):
                    field_in_hierarchy = level.get("column")
                    if field_in_hierarchy:
                        fields_in_hierarchies.add(field_in_hierarchy)
            
            if columns:
                final_config.append({
                    "table": table_name,
                    "fields": columns,
                    "measures_in_table": measures_in_table,
                    "fields_in_hierarchies": list(fields_in_hierarchies)
                })
        except (json.JSONDecodeError, KeyError, Exception) as e:
            log_and_print(f"      ❌ ERROR: Cannot process file '{table_file_path}': {e}")
    
    return final_config

# ===========================
# 🌳 FUNCTIONS FOR HIERARCHIES
# ===========================

def find_usage_in_hierarchies(tables_and_fields: List[Dict]) -> List[Dict]:
    hierarchy_results = []
    
    for tab_config in tables_and_fields:
        table_name = tab_config["table"]
        fields_in_hierarchies = tab_config.get("fields_in_hierarchies", [])
        
        for field in fields_in_hierarchies:
            hierarchy_results.append({
                'field': f"{table_name}.{field}",
                'usage_type': 'HIERARCHY',
                'page': 'Model Structure',
                'object_name': 'Drill-down navigation',
                'file': f'{table_name}.json',
                'method': 'HIERARCHY_SCAN'
            })
    
    return hierarchy_results

# ===========================
# 📊 FUNCTIONS FOR TABULAR EDITOR
# ===========================

def load_measures_from_tabular_editor(folder_path: str, tables_and_fields: List[Dict], tabular_model_path: str) -> Dict[str, str]:
    """
    Loads measure definitions from Tabular Editor files and model structure.
    Accepts tabular_model_path as a parameter.
    """
    measure_definitions = {}
    
    if folder_path and Path(folder_path).exists():
        try:
            file_patterns = ["*.dax", "*.txt", "*.sql", "*.json", "*.xml", "*.measure", "*"]
            all_files = [p for w in file_patterns for p in glob.glob(os.path.join(folder_path, "**", w), recursive=True) if os.path.isfile(p)]
            all_files = sorted(list(set(all_files)))
            
            for file_path in all_files:
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    file_name_stem = Path(file_path).stem
                    measures_from_file = extract_measures_from_text(content, file_name_stem)
                    measure_definitions.update(measures_from_file)
                except Exception as e:
                    pass
        except Exception as e:
            log_and_print(f"❌ Error loading from Tabular Editor folder: {e}")
        
    measures_from_tables = 0
    for table_config in tables_and_fields:
        table_name = table_config["table"]
        table_path = os.path.join(tabular_model_path, "tables", table_name, f"{table_name}.json")
        
        if os.path.exists(table_path):
            try:
                with open(table_path, 'r', encoding='utf-8-sig') as f:
                    table_data = json.load(f)
                
                for item in table_data.get("columns", []):
                    if item.get("type") == "calculated":
                        measure_name = item["name"]
                        expression = item.get("expression", [])
                        
                        if isinstance(expression, list):
                            expression = '\n'.join(str(line) for line in expression)
                        
                        measure_definitions[measure_name] = expression
                        measures_from_tables += 1
            except Exception as e:
                log_and_print(f"⚠️ Error loading measures from table {table_name}: {e}")
    
    log_and_print(f"✅ Loaded {len(measure_definitions)} measures for analysis.")
    
    return measure_definitions


def analyze_marts_audit(marts_path: str, reporting_path: str, fields_to_analyze: list, progress_callback=None) -> dict:
    results = {'marts_path': marts_path, 'can_comment_in_marts': [], 'cannot_comment_in_marts': [], 'errors': [], 'summary': {}}
    total_fields = len(fields_to_analyze)
    if total_fields == 0: return results

    for i, field_full in enumerate(fields_to_analyze):
        if progress_callback: progress_callback(int(((i + 1) / total_fields) * 100))
        if not isinstance(field_full, str) or '.' not in field_full: continue
        
        source_marts_model_name, field_name = field_full.split('.', 1)
        
        can_comment_marts, blocking_details_marts = can_comment_field_in_marts_final(
            field_name, source_marts_model_name, marts_path
        )

        can_comment_reporting, blocking_details_reporting = can_comment_field_in_marts_final(
            field_name, source_marts_model_name, reporting_path
        )
        
        if can_comment_marts and can_comment_reporting:
            results['can_comment_in_marts'].append({'field': field_full, 'source_model': source_marts_model_name})
        else:
            all_blocking_details = blocking_details_marts + blocking_details_reporting
            blocking_files = list(set([info['file'] for info in all_blocking_details]))
            full_blocking_context = [f"{info['file']}: {info['context']}" for info in all_blocking_details]
            results['cannot_comment_in_marts'].append({
                'field': field_full, 'source_model': source_marts_model_name,
                'blocking_models': blocking_files, 'blocking_details': full_blocking_context
            })

    results['summary'] = {'can_optimize': len(results['can_comment_in_marts']), 'blocked': len(results['cannot_comment_in_marts']), 'errors': len(results['errors'])}
    return results



def comment_out_fields_in_marts_audit(marts_path: str, fields_to_comment: list) -> dict:
    if not fields_to_comment:
        return {'commented_count': 0, 'failed_count': 0, 'summary': 'No fields to comment.'}

    fields_by_model = {}
    for field_full_name in fields_to_comment:
        model_name, field_name = field_full_name.split('.', 1)
        if model_name not in fields_by_model:
            fields_by_model[model_name] = []
        fields_by_model[model_name].append(field_name)

    commented_count = 0
    failed_count = 0
    
    for model_name, field_aliases in fields_by_model.items():
        print(f"\n[MARTS AUDIT WRAPPER] Processing model: {model_name}")
        sql_file_path = find_dbt_file_for_alias(model_name, marts_path)
        
        if not sql_file_path:
            print(f"   [ERROR] SQL file not found for model '{model_name}'. Skipping {len(field_aliases)} fields.")
            failed_count += len(field_aliases)
            continue
        
        print(f"   [MARTS AUDIT WRAPPER] Delegating modification of {os.path.basename(sql_file_path)} to core engine.")
        success = _execute_commenting_safely(sql_file_path, field_aliases)
        
        if success:
            commented_count += len(field_aliases)
        else:
            failed_count += len(field_aliases)

    return {
        'commented_count': commented_count,
        'failed_count': failed_count,
        'summary': f"Processed {len(fields_to_comment)} fields. Success: {commented_count}, Failed: {failed_count}."
    }




def extract_measures_from_text(text: str, file_name: str) -> Dict[str, str]:
    measures = {}
    pattern1 = re.findall(r"MEASURE\s+(?:'[^']*')?\[?([^\]]+)\]?\s*=\s*(.+?)(?=(?:MEASURE|\Z))", text, re.IGNORECASE | re.DOTALL)
    for name, expression in pattern1:
        measures[name.strip().replace('[', '').replace(']', '')] = expression.strip()

    try:
        if text.strip().startswith(('{', '[')):
            json_data = json.loads(text)
            extract_measures_from_json_recursively(json_data, measures)
    except json.JSONDecodeError: pass

    if not measures and len(text.strip()) > 10 and any(k in text.upper() for k in ['CALCULATE', 'SUM', 'FILTER', 'VAR', 'RETURN']):
        measures[file_name] = text.strip()
    return measures

def extract_measures_from_json_recursively(obj: Any, measures: Dict[str, str]):
    if isinstance(obj, dict):
        if ('name' in obj and 'expression' in obj) or ('Name' in obj and 'Expression' in obj):
            name = obj.get('name') or obj.get('Name')
            expression = obj.get('expression') or obj.get('Expression')
            if isinstance(expression, list): expression = ' '.join(str(item) for item in expression)
            if name and isinstance(expression, str): measures[name] = expression
        for v in obj.values(): extract_measures_from_json_recursively(v, measures)
    elif isinstance(obj, list):
        for el in obj: extract_measures_from_json_recursively(el, measures)

def analyze_measure_dependencies(measure_definitions: Dict[str, str], fields_to_search: List[str], 
                               detailed_logging=False) -> Dict[str, Any]:
    #Analyzes measure dependencies with optional detailed logging.

    basic_dependencies = {name: set() for name in measure_definitions}
    
    for measure_name, dax_definition in measure_definitions.items():
        for field in fields_to_search:
            base_field_name = field.split('.')[-1]
            if re.search(r'(\'|\[|\s|\()' + re.escape(base_field_name) + r'(\'|\]|\s|\)|\,)', 
                        dax_definition, re.IGNORECASE):
                basic_dependencies[measure_name].add(field)

        for other_measure in measure_definitions:
            if other_measure != measure_name and f"[{other_measure}]" in dax_definition:
                basic_dependencies[measure_name].add(f"MEASURE:{other_measure}")
    
    if not detailed_logging:
        return basic_dependencies
    
    # 🆕 ENHANCED: Detailed analysis
    log_and_print("📊 Generating detailed measure dependency analysis...")
    
    detailed_dependencies = {}
    
    for measure_name, deps in basic_dependencies.items():
        if not deps:  
            continue
            
        # 🆕 Detailed analysis 
        measure_info = {
            'dax_length': len(measure_definitions[measure_name]),
            'complexity_score': _calculate_dax_complexity(measure_definitions[measure_name]),
            'field_count': len([d for d in deps if not d.startswith('MEASURE:')]),
            'measure_count': len([d for d in deps if d.startswith('MEASURE:')]),
        }
        
        
        field_locations = {}
        dax_text = measure_definitions[measure_name]
        for dep in deps:
            if not dep.startswith('MEASURE:'):
                field_name = dep.split('.')[-1]
                positions = []
                start_pos = 0
                while True:
                    pos = dax_text.find(field_name, start_pos)
                    if pos == -1:
                        break
                    positions.append(pos)
                    start_pos = pos + 1
                if positions:
                    field_locations[dep] = positions[:5]  # Max 5 pozycji
        
        detailed_dependencies[measure_name] = {
            'basic_dependencies': deps,
            'measure_info': measure_info,
            'field_locations': field_locations,
            'dependency_depth': _calculate_dependency_depth(measure_name, basic_dependencies),
        }
    
    
    global_stats = {
        'total_measures': len(measure_definitions),
        'measures_with_dependencies': len([m for m in basic_dependencies.values() if m]),
        'isolated_measures': [name for name, deps in basic_dependencies.items() if not deps],
        'most_referenced_fields': _find_most_referenced_fields(basic_dependencies),
        'average_dependencies_per_measure': sum(len(deps) for deps in basic_dependencies.values()) / len(basic_dependencies) if basic_dependencies else 0,
        'max_dependencies': max(len(deps) for deps in basic_dependencies.values()) if basic_dependencies else 0
    }
    
    result = {
        'detailed_dependencies': detailed_dependencies,
        'global_statistics': global_stats,
        'basic_dependencies': basic_dependencies,  # For backward compatibility
        'is_detailed': True
    }
    
    log_and_print(f"   📈 Analyzed {len(detailed_dependencies)} measures with dependencies")
    log_and_print(f"   📊 Found {len(global_stats['isolated_measures'])} isolated measures")
    log_and_print(f"   📊 Average dependencies per measure: {global_stats['average_dependencies_per_measure']:.1f}")
    
    return result

def find_indirect_usage_by_measures(direct_results: List[Dict], measure_dependencies: Dict[str, Set[str]], fields_to_search: List[str]) -> Dict[str, Set[str]]:
    indirect_usage = {field: set() for field in fields_to_search}
    measures_on_report = {w.get('object_name') for w in direct_results if w['usage_type'].lower() in ['measure', 'visualization'] and w.get('object_name')}
    for measure in measure_dependencies: measures_on_report.add(measure)

    def find_dependent_fields(measure: str, visited: Set[str]) -> Set[str]:
        if measure in visited: return set()
        visited.add(measure)
        dependent_fields = set()
        if measure in measure_dependencies:
            for dependency in measure_dependencies[measure]:
                if dependency.startswith("MEASURE:"):
                    dependent_fields.update(find_dependent_fields(dependency[8:], visited.copy()))
                else:
                    dependent_fields.add(dependency)
        return dependent_fields

    for measure in measures_on_report:
        fields = find_dependent_fields(measure, set())
        for field in fields:
            if field in indirect_usage:
                indirect_usage[field].add(measure)
    return {k: v for k, v in indirect_usage.items() if v}

# ===========================
# 🔍 FUNCTIONS FOR FINDING FIELD USAGE
# ===========================

def extract_object_name(content: str, file_name: str) -> str:
    try:
        data = json.loads(content)
        
        # Strategy 1: Look for visual type + name combination
        visual_info = {}
        
        def extract_visual_details(obj, path=""):
            if isinstance(obj, dict):
                # Collect visual type information
                if 'visualType' in obj:
                    visual_info['type'] = obj['visualType']
                
                # Look for meaningful names (generic search)
                name_fields = ['title', 'name', 'displayName', 'caption', 'label']
                for field in name_fields:
                    if field in obj and isinstance(obj[field], str):
                        name = obj[field].strip()
                        # Skip GUIDs, IDs, and empty values
                        if (name and len(name) > 1 and 
                            not any(skip in name.lower() for skip in ['guid', 'uuid', 'id:', '{']) and
                            not name.isdigit()):
                            visual_info['name'] = name
                            break
                
                # Recurse into nested objects
                for key, value in obj.items():
                    extract_visual_details(value, f"{path}.{key}" if path else key)
                    
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    extract_visual_details(item, f"{path}[{i}]")
        
        # Extract all visual information
        extract_visual_details(data)
        
        # Strategy 2: Build meaningful object name
        visual_type = visual_info.get('type', '').replace('visual', '').title()
        visual_name = visual_info.get('name', '')
        
        # Create descriptive name
        if visual_name and visual_type:
            return f"{visual_name} ({visual_type})"
        elif visual_type:
            return f"{visual_type} Visual"
        elif visual_name:
            return visual_name
            
    except (json.JSONDecodeError, Exception):
        pass
    
    # Fallback: Clean filename
    clean_name = file_name.split('/')[-1].replace('.json', '')
    if clean_name and clean_name != 'unknown':
        return f"Object ({clean_name})"
    
    return 'Unknown Object'

def extract_page_name(file_path: str) -> str:
    """🆕 ENHANCED: Generyczne wykrywanie nazw stron z Power BI"""
    # Normalize path separators
    normalized_path = file_path.replace('\\', '/')
    parts = normalized_path.split('/')
    
    try:
        # Strategy 1: Extract from sections/[PAGE_ID] structure  
        if 'sections' in parts:
            section_idx = parts.index('sections')
            if section_idx + 1 < len(parts):
                page_id = parts[section_idx + 1]
                
                # Generic page naming without hardcodes
                if page_id.isdigit():
                    page_num = int(page_id) + 1  # 0-based to 1-based
                    return f"Page {page_num}"
                elif page_id.startswith('ReportSection'):
                    # Extract number from ReportSection123abc -> "Report Page 123"
                    import re
                    match = re.search(r'ReportSection(\d*)', page_id)
                    if match and match.group(1):
                        return f"Report Page {match.group(1)}"
                    else:
                        return "Report Page 1"
                else:
                    # Use page_id as-is but clean it up
                    clean_id = page_id.replace('_', ' ').title()
                    return f"Page ({clean_id})"
        
        # Strategy 2: Look for page indicators in path
        file_lower = normalized_path.lower()
        if 'bookmark' in file_lower:
            return 'Bookmarks'
        elif 'filter' in file_lower:
            return 'Filters Panel'
        elif 'mobile' in file_lower:
            return 'Mobile Layout'
            
    except (ValueError, IndexError):
        pass
    
    # Fallback: Extract from filename
    if parts:
        filename = parts[-1].replace('.json', '')
        if filename and len(filename) > 0:
            return f"Section ({filename})"
    
    return 'Unknown Page'

def is_valid_field_reference(content: str, variant: str, field_name: str) -> bool:
    for line in content.split('\n'):
        if variant in line:
            line_lower = line.lower().strip()
            valid_contexts = [
                'queryref', 'datafield', 'column', 'property', 'sourceref', 'expression', 
                'measure', 'filter', 'entity', 'name', 'table', 'select', 'from', 'where',
                'prototypequery', 'source', 'aggregation'
            ]
            if any(context in line_lower for context in valid_contexts):
                if 'comment' not in line_lower and 'description' not in line_lower:
                    return True
    return False

def determine_usage_type(content: str, variant: str, file_name: str) -> str:
    context = ' '.join([line.lower() for line in content.split('\n') if variant in line])
    if any(s in context for s in ['filter', 'slicer', 'where']): return 'FILTER'
    if any(s in context for s in ['visual', 'chart', 'table', 'matrix', 'select', 'prototypequery']): return 'VISUALIZATION'
    if any(s in context for s in ['measure', 'sum(', 'count(', 'calculate(', 'aggregation']): return 'MEASURE'
    if 'filter' in file_name.lower(): return 'FILTER'
    return 'OTHER'

def search_single_pbix_for_field_usage(zip_path: str, tables_and_fields: List[Dict], detailed_logging=False) -> List[Dict]:
    
    #Searches for field usage in PBIX files.
    

    results = []
    found_unique = set() 
    
    if not Path(zip_path).exists():
        log_and_print(f"❌ PBIX file not found: {zip_path}")
        return results

    if detailed_logging:
        log_and_print("📊 Detailed logging mode: collecting enhanced context data...")

    field_variants = {}
    for table_config in tables_and_fields:
        table_name = table_config["table"]
        for field_name in table_config["fields"]:
            field_key = f"{table_name}.{field_name}"
            variants = {
                f"{table_name}.{field_name}",
                f"'{table_name}'.'{field_name}'",
                f"[{table_name}].[{field_name}]",
                f'"{table_name}"."{field_name}"',
                f"'{table_name}'[{field_name}]",
                field_name,
                f"'{field_name}'",
                f'"{field_name}"',
                f"[{field_name}]"
            }
            field_variants[field_key] = list(variants)

    hierarchy_results = find_usage_in_hierarchies(tables_and_fields)
    for result in hierarchy_results:
        unique_key = (result['field'], result['usage_type'], result['page'])
        if unique_key not in found_unique:
            found_unique.add(unique_key)
            
            if detailed_logging:
                result.update({
                    'full_context': 'Hierarchy definition in model structure',
                    'line_number': 'N/A - Model Metadata',
                    'exact_expression': f"Hierarchy level for {result['field']}",
                    'confidence_score': 100,
                    'detection_details': 'Found in table JSON hierarchy definition',
                    'usage_context': 'Navigation Structure'
                })
            
            results.append(result)

    with zipfile.ZipFile(zip_path, 'r') as zipf:
        json_files = [f for f in zipf.namelist() if f.endswith('.json') and not any(p in f.lower() for p in ['bookmark', 'resources'])]
        
        if detailed_logging:
            log_and_print(f"📁 Processing {len(json_files)} JSON files for detailed analysis...")
        
        for file_name in json_files:
            try:
                with zipf.open(file_name) as f:
                    content = f.read().decode('utf-8')
                
                try:
                    json_data = json.loads(content)
                    structural_findings = find_fields_in_json_structure(json_data, tables_and_fields)
                    
                    for full_field_name, usage_type, context in structural_findings:
                        page = extract_page_name(file_name)
                        obj = extract_object_name(content, file_name)
                        
                        unique_key = (full_field_name, usage_type, page)
                        if unique_key not in found_unique:
                            found_unique.add(unique_key)
                            
                            result_data = {
                                'field': full_field_name, 
                                'usage_type': usage_type,
                                'page': page, 
                                'object_name': obj,
                                'file': os.path.basename(file_name),
                                'method': f'JSON_STRUCTURE_{context}'
                            }
                            
                            if detailed_logging:

                                table_name, field_name = full_field_name.split('.', 1) if '.' in full_field_name else ('', full_field_name)
                                usage_context = extract_usage_context_simple(json_data, field_name, table_name)
                                
                                result_data.update({
                                    'full_context': f'JSON structural reference in {context}',
                                    'line_number': 'N/A - JSON Structure',
                                    'exact_expression': f'Structural reference: {context}',
                                    'confidence_score': 90,  
                                    'detection_details': f'JSON structural analysis: {context}',
                                    'file_size': len(content),
                                    'usage_context': usage_context  
                                })
                            
                            results.append(result_data)
                            
                except (json.JSONDecodeError, Exception):
                    if detailed_logging:
                        log_and_print(f"⚠️ Could not parse JSON in {os.path.basename(file_name)}")

                for field_key, variants in field_variants.items():
                    field_name = field_key.split('.')[-1]
                    for variant in variants:
                        if variant in content:
                            if not is_valid_field_reference(content, variant, field_name):
                                continue
                            
                            usage_type = determine_usage_type(content, variant, file_name)
                            page = extract_page_name(file_name)
                            obj = extract_object_name(content, file_name)
                            
                            unique_key = (field_key, usage_type, page)
                            if unique_key not in found_unique:
                                found_unique.add(unique_key)
                                
                                result_data = {
                                    'field': field_key, 
                                    'usage_type': usage_type,
                                    'page': page, 
                                    'object_name': obj,
                                    'file': os.path.basename(file_name),
                                    'method': 'TEXT_SEARCH'
                                }
                                
                                if detailed_logging:
                                    lines = content.split('\n')
                                    line_num = 'N/A'
                                    line_content = 'Line not found'
                                    for i, line in enumerate(lines):
                                        if variant in line:
                                            line_num = str(i + 1)
                                            line_content = line.strip()[:100]
                                            break
                                    
                                    variant_pos = content.find(variant)
                                    start_pos = max(0, variant_pos - 50)
                                    end_pos = min(len(content), variant_pos + len(variant) + 50)
                                    surrounding_context = content[start_pos:end_pos].replace(variant, f">>>{variant}<<<")
                                    
                                    table_name, field_name = field_key.split('.', 1) if '.' in field_key else ('', field_key)
                                    try:
                                        json_data = json.loads(content)
                                        usage_context = extract_usage_context_simple(json_data, field_name, table_name)
                                    except:
                                        usage_context = 'Text Search Context'
                                    
                                    result_data.update({
                                        'full_context': surrounding_context,
                                        'line_number': line_num,
                                        'exact_expression': line_content,
                                        'confidence_score': 75, 
                                        'detection_details': f'Text search for variant: {variant}',
                                        'variant_matched': variant,
                                        'field_variations_count': len(variants),
                                        'usage_context': usage_context
                                    })
                                
                                results.append(result_data)
                                break
                                
            except Exception as e:
                if detailed_logging:
                    log_and_print(f"⚠️ Error processing file {os.path.basename(file_name)}: {e}")

    if detailed_logging:
        log_and_print(f"📊 Detailed analysis completed:")
        log_and_print(f"   Total results: {len(results)}")
    
    return results

def search_for_field_usage(zip_paths: List[str], tables_and_fields: List[Dict], detailed_logging=False) -> List[Dict]:
    """
    Searches for field usage across multiple PBIX files.
    Aggregates results - field is considered used if found in ANY report.
    """
    if not zip_paths:
        log_and_print("❌ No PBIX files provided")
        return []
    
    log_and_print(f"🔍 Analyzing {len(zip_paths)} PBIX file(s)...")
    
    all_results = []
    found_unique = set()
    file_stats = {}
    
    for i, zip_path in enumerate(zip_paths):
        file_name = Path(zip_path).name
        log_and_print(f"📊 Processing file {i+1}/{len(zip_paths)}: {file_name}")
        
        try:
            single_results = search_single_pbix_for_field_usage(zip_path, tables_and_fields, detailed_logging)
            file_stats[file_name] = len(single_results)
            
            new_findings = 0
            for result in single_results:
                field = result.get('field', '')
                usage_type = result.get('usage_type', '')
                page = result.get('page', '')
                
                unique_key = (field, usage_type, page)
                
                if unique_key not in found_unique:
                    found_unique.add(unique_key)
                    result['source_file'] = file_name
                    all_results.append(result)
                    new_findings += 1
                    
            log_and_print(f"   ✅ Found {len(single_results)} usages, {new_findings} new unique findings")
                    
        except Exception as e:
            log_and_print(f"❌ Error processing {file_name}: {e}")
            file_stats[file_name] = f"ERROR: {str(e)[:50]}"
            continue
    
    log_and_print(f"🎯 MULTI-PBIX ANALYSIS COMPLETE:")
    log_and_print(f"   📁 Files processed: {len(zip_paths)}")
    log_and_print(f"   ✅ Unique field usages: {len(all_results)}")
    log_and_print(f"   📊 Per-file breakdown:")
    
    for file_name, count in file_stats.items():
        if isinstance(count, int):
            log_and_print(f"      • {file_name}: {count} usages")
        else:
            log_and_print(f"      • {file_name}: {count}")
    
    return all_results


def search_for_relationships(tabular_model_path: str, all_fields: List[str]) -> Dict[str, bool]:
    """
    FINAL, SIMPLIFIED AND ROBUST VERSION. This function aggressively scans ALL
    .json and .bim files within the path to find relationship definitions,
    whether they are in a central list, an annotation, or individual files.
    """
    relationships = {field: False for field in all_fields}
    columns_in_relationships = set()
    
    if not tabular_model_path or not Path(tabular_model_path).exists():
        print("[WARNING] Tabular model path not found, cannot search for relationships.")
        return relationships

    print("   🔍 Aggressively scanning for relationships in all .json and .bim files...")
    

    all_files_to_check = glob.glob(os.path.join(tabular_model_path, "**", "*.json"), recursive=True) + \
                         glob.glob(os.path.join(tabular_model_path, "**", "*.bim"), recursive=True)

    if not all_files_to_check:
        print("   ❌ CRITICAL WARNING: No .json or .bim files found in the specified path.")
        return relationships
        
    all_found_relationships = []
    
    for file_path in all_files_to_check:
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                model_data = json.load(f)

            if os.path.basename(os.path.dirname(file_path)) == 'relationships':
                if isinstance(model_data, dict):
                    all_found_relationships.append(model_data)
                continue # Przejdź do następnego pliku

            found_rels = find_key_recursively(model_data, 'relationships')
            if isinstance(found_rels, list):
                all_found_relationships.extend(found_rels)

            annotations = find_key_recursively(model_data, 'annotations')
            if isinstance(annotations, list):
                for annotation in annotations:
                    if isinstance(annotation, dict) and annotation.get('name') == 'TabularEditor_Relationships':
                        value = annotation.get('value')
                        if isinstance(value, list) and all(isinstance(i, str) for i in value):
                            try:
                                json_string = "".join(value)
                                nested_rels = json.loads(json_string)
                                if isinstance(nested_rels, list):
                                    all_found_relationships.extend(nested_rels)
                            except json.JSONDecodeError:
                                pass
                        elif isinstance(value, list):
                             all_found_relationships.extend(value)
        except Exception:

            pass

    if all_found_relationships:
        for rel in all_found_relationships:
            if isinstance(rel, dict) and all(k in rel for k in ['fromTable', 'fromColumn', 'toTable', 'toColumn']):
                columns_in_relationships.add(f"{rel['fromTable']}.{rel['fromColumn']}")
                columns_in_relationships.add(f"{rel['toTable']}.{rel['toColumn']}")
        print(f"   ✅ Success. Found a total of {len(all_found_relationships)} relationships across all scanned files.")
    else:
        print("   ❌ CRITICAL WARNING: No relationships found. Foreign Keys will be incorrectly marked as unused.")
    
    for field in all_fields:
        if field in columns_in_relationships:
            relationships[field] = True
            
    return relationships

def find_usage_in_sort_by_column(tables_and_fields: List[Dict], tabular_model_path: str) -> Set[str]:
    sorting_columns = set()
    
    if not tabular_model_path or not Path(tabular_model_path).exists():
        return sorting_columns

    print("   🔍 Searching for 'Sort By Column' usage in Tabular model...")

    for table_config in tables_and_fields:
        table_name = table_config.get("table")
        if not table_name:
            continue

        table_file_path = os.path.join(tabular_model_path, "tables", table_name, f"{table_name}.json")
        
        if not os.path.exists(table_file_path):
            continue
            
        try:
            with open(table_file_path, 'r', encoding='utf-8-sig') as f:
                table_data = json.load(f)
            
            json_column_list = table_data.get("columns", [])
            
            for column_definition in json_column_list:
                if "sortByColumn" in column_definition:
                    sort_by_column_name = column_definition["sortByColumn"]
                    
                    full_name = f"{table_name}.{sort_by_column_name}"
                    
                    if full_name not in sorting_columns:
                        print(f"      ✅ Found sorting usage: '{full_name}' sorts column '{column_definition['name']}'")
                        sorting_columns.add(full_name)

        except (json.JSONDecodeError, KeyError, Exception) as e:
            pass
            
    return sorting_columns

def find_usage_in_rls_filters(tabular_model_path: str) -> Set[str]:
    rls_columns = set()
    roles_path = os.path.join(tabular_model_path, "roles")

    if not os.path.exists(roles_path):
        return rls_columns

    print("   🔒 Searching for RLS (Row-Level Security) usage...")

    dax_column_pattern = re.compile(r"'([^']*)'\[([^\]]*)\]")

    for role_file in glob.glob(os.path.join(roles_path, "*.json")):
        try:
            with open(role_file, 'r', encoding='utf-8-sig') as f:
                role_data = json.load(f)

            table_permissions = role_data.get("tablePermissions", [])
            for permission in table_permissions:
                filter_expression = permission.get("filterExpression")
                if filter_expression:
                    matches = dax_column_pattern.findall(filter_expression)
                    for table_name, column_name in matches:
                        full_name = f"{table_name}.{column_name}"
                        if full_name not in rls_columns:
                            print(f"      ✅ Found RLS usage: '{full_name}' in role '{role_data.get('name')}'")
                            rls_columns.add(full_name)

        except (json.JSONDecodeError, KeyError, Exception) as e:
            pass

    return rls_columns

# ===========================
# 🚀 FUNCTIONS FOR DISPLAYING RESULTS
# ===========================

def generate_usage_details(details: Dict[str, List[Dict]]) -> str:
    fragments = []
    
    if details.get('visualization'):
        pages = set(detail['page'][:15] for detail in details['visualization'])
        viz_text = "Viz: " + ", ".join(sorted(pages)[:2])
        if len(pages) > 2: viz_text += f" +{len(pages)-2}"
        fragments.append(viz_text)
    
    if details.get('measure'):
        objects = set(detail['object'][:15] for detail in details['measure'] if detail['object'] != 'Unknown Object')
        if objects:
            measure_text = "Measure: " + ", ".join(sorted(objects)[:2])
            if len(objects) > 2: measure_text += f" +{len(objects)-2}"
            fragments.append(measure_text)
    
    if details.get('filter'):
        pages = set(detail['page'][:15] for detail in details['filter'])
        if pages:
            filter_text = "Filter: " + ", ".join(sorted(pages)[:2])
            if len(pages) > 2: filter_text += f" +{len(pages)-2}"
            fragments.append(filter_text)
    
    if details.get('hierarchy'):
        fragments.append("Hierarchy: Drill-down")
    
    if details.get('indirect_measure'):
        measures = set(detail['object'].replace('Measure: ', '')[:15] for detail in details['indirect_measure'])
        if measures:
            indirect_text = "→Measure: " + ", ".join(sorted(measures)[:2])
            if len(measures) > 2: indirect_text += f" +{len(measures)-2}"
            fragments.append(indirect_text)
    
    if details.get('relationship'):
        fragments.append("Relationship: FK/PK")
    
    result = " | ".join(fragments)
    return (result[:55] + "...") if len(result) > 58 else result if result else "No details"

def display_results(results: List[Dict], relationships: Dict[str, bool] = None, indirect_usage: Dict[str, Set[str]] = None) -> None:
    if indirect_usage is None: indirect_usage = {}
    if relationships is None: relationships = {}

    all_fields = [f"{k['table']}.{p}" for k in TABLES_AND_FIELDS for p in k['fields']]
    
    field_summary = {
        field: {'details': {'visualization': [], 'measure': [], 'filter': [], 'other': [], 'indirect_measure': [], 'relationship': [], 'hierarchy': []}} 
        for field in all_fields
    }
    
    for result in results:
        field = result['field']
        if field in field_summary:
            usage_type = result['usage_type'].lower()
            if usage_type not in field_summary[field]['details']:
                 field_summary[field]['details'][usage_type] = []
            
            field_summary[field]['details'][usage_type].append({
                'page': result['page'], 'object': result.get('object_name', 'Unknown Object')
            })
    
    for field, measures in indirect_usage.items():
        if field in field_summary:
            for measure in measures:
                field_summary[field]['details']['indirect_measure'].append({'object': f'Measure: {measure}'})
    
    for field, has_relationship in relationships.items():
        if has_relationship and field in field_summary:
            field_summary[field]['details']['relationship'].append({})

    print(f"\n📊 FIELD USAGE SUMMARY")
    print("=" * 210)
    print(f"{'Field':<65} {'Used':<8} {'Viz':<4} {'Measure':<6} {'Filter':<6} {'Ind.Meas':<8} {'Relation':<8} {'Hierarchy':<10} {'Usage Details':<60}")
    print("-" * 210)
    
    used_counter = 0
    fields_to_skip = ['PartitionDate', 'LastProcessingDate', 'RangeModificationDate', 'TableIndicator']
    
    stats = {'visualization': 0, 'measure': 0, 'filter': 0, 'hierarchy': 0, 'indirect_measure': 0, 'relationship': 0}
    
    fields_by_table = {}
    for field in all_fields:
        if field.split('.')[-1] in fields_to_skip: continue
        table = field.split('.', 1)[0]
        
        is_excluded, _ = is_table_excluded(table)
        if is_excluded: continue
        
        if table not in fields_by_table: fields_by_table[table] = []
        fields_by_table[table].append(field)
    
    for table in sorted(fields_by_table.keys()):
        for i, field in enumerate(sorted(fields_by_table[table])):
            usage_details = field_summary.get(field, {}).get('details', {})
            has_relationship = bool(usage_details.get('relationship'))
            
            is_used = any(usage_details.values())
            if is_used: used_counter += 1

            for type in ['visualization', 'measure', 'filter', 'hierarchy', 'indirect_measure', 'relationship']:
                if usage_details.get(type): stats[type] +=1

            used_str = '✅' if is_used else '❌'
            viz_str = '✅' if usage_details.get('visualization') else '❌'
            measure_str = '✅' if usage_details.get('measure') else '❌'
            filter_str = '✅' if usage_details.get('filter') else '❌'
            indirect_str = '✅' if usage_details.get('indirect_measure') else '❌'
            relationship_status = '✅' if has_relationship else '❌'
            hierarchy_str = '✅' if usage_details.get('hierarchy') else '❌'
            
            details_text = generate_usage_details(usage_details)
            
            print(f"{field:<65} {used_str:<8} {viz_str:<4} {measure_str:<6} {filter_str:<6} {indirect_str:<8} {relationship_status:<8} {hierarchy_str:<10} {details_text:<60}")
            
            if i == len(fields_by_table[table]) - 1:
                print("=" * 210)

    all_fields_filtered = []
    for p in all_fields:
        if p.split('.')[-1] in ['PartitionDate', 'LastProcessingDate', 'RangeModificationDate', 'TableIndicator']:
            continue
        table = p.split('.', 1)[0]
        is_excluded, _ = is_table_excluded(table)
        if not is_excluded:
            all_fields_filtered.append(p)

    total_fields = len(all_fields_filtered)
    percentage_used = (used_counter / total_fields * 100) if total_fields > 0 else 0
    
    print("\n📈 STATISTICAL SUMMARY:")
    print(f"   Analyzed fields:   {total_fields}")
    print(f"   Used fields:       {used_counter} ({percentage_used:.1f}%)")
    print(f"   ├─ In visualizations: {stats['visualization']}")
    print(f"   ├─ In measures (dir.): {stats['measure']}")  
    print(f"   ├─ In filters:        {stats['filter']}")
    print(f"   ├─ In hierarchies:    {stats['hierarchy']}  🌳")
    print(f"   ├─ Indirectly (meas.): {stats['indirect_measure']}")
    print(f"   └─ In relationships:  {stats['relationship']}")
    print(f"   Unused fields:     {total_fields - used_counter}")

def show_field_details(field_name: str, results: List[Dict], relationships: Dict[str, bool] = None, indirect_usage: Dict[str, Set[str]] = None) -> None:
    if relationships is None: relationships = {}
    if indirect_usage is None: indirect_usage = {}
    
    print(f"\n🔍 DETAILED ANALYSIS OF FIELD: {field_name}")
    print("=" * 100)
    
    usage_found = False
    
    for result in results:
        if result['field'] == field_name:
            usage_found = True
            print(f"🔎 {result['usage_type']}")
            print(f"   Page: {result['page']}")
            print(f"   Object: {result.get('object_name', 'Unknown')}")
            print(f"   File: {result.get('file', 'Unknown')}")
            print(f"   Detection Method: {result.get('method', 'UNKNOWN')}")
            print()
            
    if field_name in indirect_usage:
        usage_found = True
        print(f"🔗 INDIRECT USAGE VIA MEASURES:")
        for measure in indirect_usage[field_name]:
            print(f"   → {measure}")
        print()
    
    if relationships.get(field_name, False):
        usage_found = True
        print(f"🔑 RELATIONSHIPS:")
        print(f"   Field used as a key in data model relationships")
        print()
    
    if not usage_found:
        print("❌ FIELD IS NOT USED")
    
    print("=" * 100)

# ===========================
# 🔧 FIXED ALIAS AND DBT PARSERS
# ===========================

def find_snowflake_alias_for_table(table_name: str, tabular_model_path: str) -> str:
    """Find Snowflake alias for a table - improved Power Query M parser"""
    table_path = os.path.join(tabular_model_path, "tables", table_name, f"{table_name}.json")
    
    if not os.path.exists(table_path):
        return ""
    
    try:
        with open(table_path, 'r', encoding='utf-8-sig') as f:
            table_data = json.load(f)
        
        partitions = table_data.get('partitions', [])
        for partition in partitions:
            if isinstance(partition, dict):
                source = partition.get('source', {})
                if isinstance(source, dict):
                    expression_lines = source.get('expression', [])
                    if isinstance(expression_lines, list):
                        expression_str = '\n'.join(str(line) for line in expression_lines)
                        
                        # Bardziej agresywne wzorce do szukania nazwy tabeli/widoku
                        patterns = [
                            r'Source{[^}]+Item="([^"]+)"',
                            r'\[Name="([^"]+)",Kind="Table"\]',
                            r'\[Name="([^"]+)",Kind="View"\]',
                            r'#"(Dim[^"]+)"',
                            r'#"(Fact[^"]+)"'
                        ]
                        
                        for pattern in patterns:
                            match = re.search(pattern, expression_str, re.IGNORECASE)
                            if match:
                                alias = match.group(1)
                                if alias.lower() not in ['public', 'reporting_fka', 'core_fka', 'marts_fka']:
                                    return alias
        
        if not table_name.startswith(('Dim', 'Fact', 'Bridge')):
            return f"Dim{table_name}"
        return table_name
        
    except Exception as e:
        return ""

def find_dbt_file_for_alias(alias: str, dbt_models_path: str) -> str:
    if not os.path.exists(dbt_models_path) or not alias:
        return ""
    
    # --- STRATEGY 1: SEARCH BY CONTENT (UNCHANGED) ---
    all_sql_files = glob.glob(os.path.join(dbt_models_path, "**", "*.sql"), recursive=True)
    for file_path in all_sql_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if f"alias='{alias}'" in content or f'alias="{alias}"' in content:
                print(f"   [STRATEGY 1] Found file by alias in config: {os.path.basename(file_path)}")
                return file_path
        except Exception:
            continue
    
    # --- IMPROVED STRATEGY 2: SEARCH BY FILENAME (FROM MOST PRECISE) ---

    # NEW - PATTERN 2.1: Search for a file in a subdirectory of the same name
    exact_path_in_subdir = os.path.join(dbt_models_path, alias, f"{alias}.sql")
    if os.path.exists(exact_path_in_subdir):
        print(f"   [STRATEGY 2.1] Found exact path in subdirectory: {os.path.basename(exact_path_in_subdir)}")
        return exact_path_in_subdir

    # NEW - PATTERN 2.2: Search for a file with the EXACT name anywhere (recursively)
    exact_name_files = glob.glob(os.path.join(dbt_models_path, "**", f"{alias}.sql"), recursive=True)
    if exact_name_files:
        print(f"   [STRATEGY 2.2] Found file with exact name: {os.path.basename(exact_name_files[0])}")
        return exact_name_files[0]

    # --- STRATEGY 3: SEARCH BY GENERIC PATTERNS (FALLBACK) ---
    alias_clean = alias.replace('Dim', '').replace('Fact', '').replace('Bridge', '')
    name_patterns = [
        f"*{alias.lower()}*.sql",
        f"*{alias_clean.lower()}*.sql",
        f"marts_*{alias.lower()}*.sql",
        f"marts_*{alias_clean.lower()}*.sql",
    ]
    
    for pattern in name_patterns:
        files = glob.glob(os.path.join(dbt_models_path, "**", pattern), recursive=True)
        if files:
            print(f"   [STRATEGY 3] Found file by pattern '{pattern}': {os.path.basename(files[0])}")
            return files[0]
    
    print(f"   [ERROR] DBT file not found for alias '{alias}' in path {dbt_models_path}")
    return ""

def find_source_marts_model_from_reporting_file(reporting_sql_path: str) -> str:
    if not os.path.exists(reporting_sql_path):
        return ""
    try:
        with open(reporting_sql_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        match = re.search(r"ref\(['\"](marts_[^'\"]+)['\"]\)", content, re.IGNORECASE)
        if match:
            return match.group(1)
        return ""
    except Exception:
        return ""

def analyze_dbt_columns_fixed(file_path: str) -> Dict[str, str]:
    if not os.path.exists(file_path):
        return {}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

        main_select_content = _find_main_select_by_patterns(content)
        if not main_select_content:
            return {}
            
        column_definitions = _parse_column_definitions(main_select_content)
        
        dbt_columns = OrderedDict()
        for col_def in column_definitions:
            line_for_parsing = re.sub(r'--.*', '', col_def).replace('\n', ' ').replace('\r', ' ')
            line_for_parsing = ' '.join(line_for_parsing.split())
            alias = _extract_column_alias(line_for_parsing)
            
            if alias:
                dbt_columns[col_def] = alias
                
        return dbt_columns
        
    except Exception as e:
        print(f"   Error analyzing DBT columns in '{file_path}': {e}")
        return {}

def get_all_fields_from_dbt_path(path: str) -> List[Dict]:
    """
    NEW FUNCTION: Scans an entire DBT path (e.g., marts) and collects all defined fields.
    Acts as a "collector" of raw data that will be filtered later in the UI.
    
    Returns:
        A list of dictionaries, e.g., [{"field": "FieldName", "source_model": "model_name"}, ...]
    """
    if not os.path.exists(path):
        print(f"⚠️ [Field Collector] Path does not exist: {path}")
        return []

    all_fields = []
    sql_files = glob.glob(os.path.join(path, "**", "*.sql"), recursive=True)
    
    print(f"🔍 [Field Collector] Found {len(sql_files)} .sql files in '{path}' to scan.")

    for file_path in sql_files:
        try:
            # Model name is derived from the file name
            model_name = os.path.splitext(os.path.basename(file_path))[0]
            
            # We use the existing, tested function to parse columns
            dbt_columns = analyze_dbt_columns_fixed(file_path)
            
            # dbt_columns returns an OrderedDict {'sql_definition': 'alias'}
            # We are only interested in the aliases (the values)
            for pbi_alias in dbt_columns.values():
                all_fields.append({
                    "field": pbi_alias,
                    "source_model": model_name
                })
        except Exception as e:
            print(f"⚠️ [Field Collector] Error while processing file {os.path.basename(file_path)}: {e}")
            continue
            
    print(f"✅ [Field Collector] Successfully collected {len(all_fields)} fields.")
    return all_fields



def _find_main_select_by_patterns(content: str) -> str:
    select_positions = []
    
    for match in re.finditer(r'\bselect\b', content, re.IGNORECASE):
        pos = match.start()
        
        preceding = content[:pos]
        nesting_level = preceding.count('(') - preceding.count(')')
        
        if nesting_level == 0:
            select_positions.append(pos)
    
    if not select_positions:
        return ""
    
    main_select_pos = select_positions[-1]
    
    from_pos = _find_from_for_select(content, main_select_pos)
    
    if from_pos == -1:
        return content[main_select_pos:]
    else:
        return content[main_select_pos:from_pos]


def _find_from_for_select(content: str, select_pos: int) -> int:
    pos = select_pos + 6
    paren_level = 0
    
    while pos < len(content):
        char = content[pos]
        
        if char == '(':
            paren_level += 1
        elif char == ')':
            paren_level -= 1
        elif paren_level == 0:
            if content[pos:pos+4].lower() == 'from':
                before_ok = pos == 0 or not content[pos-1].isalnum()
                after_ok = pos+4 >= len(content) or not content[pos+4].isalnum()
                if before_ok and after_ok:
                    return pos
        pos += 1
    
    return -1


def _parse_column_definitions(select_content: str) -> list:
    select_content = re.sub(r'^\s*select\s+', '', select_content, flags=re.IGNORECASE).strip()
    
    column_definitions = []
    current_def = ""
    paren_count = 0
    
    for char in select_content:
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
        elif char == ',' and paren_count == 0:
            if current_def.strip():
                column_definitions.append(current_def.strip())
            current_def = ""
            continue
        current_def += char
    
    if current_def.strip():
        column_definitions.append(current_def.strip())
        
    return column_definitions


def _extract_column_alias(line_for_parsing: str) -> str:
    alias = ""
    
    as_match = re.search(r'as\s+("([^"]+)"|([a-zA-Z0-9_]+))\s*$', line_for_parsing, re.IGNORECASE)
    if as_match:
        alias = as_match.group(2) or as_match.group(3)
    else:
        simple_match = re.search(r'("([^"]+)"|([a-zA-Z0-9_]+))\s*$', line_for_parsing, re.IGNORECASE)
        if simple_match:
            alias = simple_match.group(2) or simple_match.group(3)
    
    return alias

def _extract_select_block(content: str, select_pos: int) -> str:
    if select_pos == -1:
        return ""
    
    from_pos = -1
    current_pos = select_pos + 6
    paren_level = 0
    
    while current_pos < len(content):
        char = content[current_pos]
        if char == '(':
            paren_level += 1
        elif char == ')':
            paren_level -= 1
        elif paren_level == 0:
            remaining = content[current_pos:current_pos + 4]
            if remaining.lower() == 'from':
                if (current_pos == 0 or not content[current_pos - 1].isalnum()) and \
                   (current_pos + 4 >= len(content) or not content[current_pos + 4].isalnum()):
                    from_pos = current_pos
                    break
        current_pos += 1
    
    if from_pos == -1:
        return content[select_pos:]
    
    select_block = content[select_pos:from_pos]
    
    remaining_content = content[from_pos:]
    union_match = re.search(r'\bunion\s+(all\s+)?select\b', remaining_content, re.IGNORECASE)
    
    if union_match:
        union_select_pos = from_pos + union_match.start()
        second_select_pos = union_select_pos + len(union_match.group(0)) - 6  # Pozycja SELECT w UNION
        
        second_from_pos = -1
        current_pos = second_select_pos + 6
        paren_level = 0
        
        while current_pos < len(content):
            char = content[current_pos]
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1
            elif paren_level == 0:
                remaining = content[current_pos:current_pos + 4]
                if remaining.lower() == 'from':
                    if (current_pos == 0 or not content[current_pos - 1].isalnum()) and \
                       (current_pos + 4 >= len(content) or not content[current_pos + 4].isalnum()):
                        second_from_pos = current_pos
                        break
            current_pos += 1
        
        if second_from_pos != -1:
            return content[select_pos:second_from_pos]
    
    return select_block


def _parse_column_definitions(select_content: str) -> list:
    select_content = re.sub(r'^\s*select\s+', '', select_content, flags=re.IGNORECASE).strip()
    
    union_match = re.search(r'\s+union\s+(all\s+)?select\s+', select_content, re.IGNORECASE)
    if union_match:
        select_content = select_content[:union_match.start()]
    
    column_definitions = []
    current_def = ""
    paren_count = 0
    
    for char in select_content:
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
        elif char == ',' and paren_count == 0:
            if current_def.strip():
                column_definitions.append(current_def.strip())
            current_def = ""
            continue
        current_def += char
    
    if current_def.strip():
        column_definitions.append(current_def.strip())
        
    return column_definitions


def _extract_select_block(content: str, select_pos: int) -> str:
    from_match = None
    current_pos = select_pos + 6
    paren_level = 0
    
    while current_pos < len(content):
        char = content[current_pos]
        if char == '(':
            paren_level += 1
        elif char == ')':
            paren_level -= 1
        elif paren_level == 0:
            if re.match(r'\bfrom\b', content[current_pos:], re.IGNORECASE):
                from_match = current_pos
                break
        current_pos += 1
    
    if from_match:
        return content[select_pos:from_match]
    else:
        return content[select_pos:]


def _parse_column_definitions(select_content: str) -> list:
    select_content = re.sub(r'^\s*select\s+', '', select_content, flags=re.IGNORECASE).strip()
    
    union_match = re.search(r'\bunion\b', select_content, re.IGNORECASE)
    if union_match:
        select_content = select_content[:union_match.start()]
    
    column_definitions = []
    current_def = ""
    paren_count = 0
    
    for char in select_content:
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
        elif char == ',' and paren_count == 0:
            if current_def.strip():
                column_definitions.append(current_def.strip())
            current_def = ""
            continue
        current_def += char
    
    if current_def.strip():
        column_definitions.append(current_def.strip())
        
    return column_definitions


def _extract_column_alias(line_for_parsing: str) -> str:
    alias = ""
    
    as_match = re.search(r'as\s+("([^"]+)"|([a-zA-Z0-9_]+))\s*$', line_for_parsing, re.IGNORECASE)
    if as_match:
        alias = as_match.group(2) or as_match.group(3)
    else:
        simple_match = re.search(r'("([^"]+)"|([a-zA-Z0-9_]+))\s*$', line_for_parsing, re.IGNORECASE)
        if simple_match:
            alias = simple_match.group(2) or simple_match.group(3)
    
    return alias

# ===========================
# 🎯 FUNCTIONS FOR COMMENTING OUT COLUMNS
# ===========================


def comment_out_unused_columns_in_dbt(sql_file_path: str, dbt_columns: Dict[str, str], unused_column_names: List[str]) -> bool:
    """
    INTELLIGENT FINAL VERSION. This version restores the logic for finding the
    main SELECT block, ensuring that modifications ONLY happen there,
    completely ignoring any CTEs.
    """
    if not os.path.exists(sql_file_path):
        return False

    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        ends_with_newline = original_content.endswith('\n')
        lines = original_content.splitlines()
        
        main_select_blocks = _find_all_main_select_blocks_final(lines)
        if not main_select_blocks:
            print(f"   [WARNING] Could not identify a main SELECT block in {os.path.basename(sql_file_path)}. Skipping modification.")
            return False 

        safe_zone = main_select_blocks[-1]
        safe_start_line = safe_zone['start']
        safe_end_line = safe_zone['end']
        
        print(f"   [DEBUG] Identified safe zone for modification: Lines {safe_start_line + 1} to {safe_end_line + 1}")

        modified_lines = list(lines)
        unused_aliases_lower = {name.lower() for name in unused_column_names}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        total_commented_count = 0

        for i in range(safe_start_line, safe_end_line):
            line = lines[i]
            stripped_line = line.strip()

            if not stripped_line or stripped_line.startswith('--') or stripped_line.startswith('/*'):
                continue


            is_potential_column_def = stripped_line.startswith(',') or stripped_line.upper().startswith('SELECT')
            if not is_potential_column_def:
                continue

            defined_alias = _get_alias_from_line_final(line)
            if defined_alias and defined_alias.lower() in unused_aliases_lower:
                indentation = line[:len(line) - len(line.lstrip())]
                line_content = line.strip()
                commented_line = f"{indentation}/* UNUSED FIELD (commented by script {timestamp}): {line_content} */"
                modified_lines[i] = commented_line
                total_commented_count += 1
        
        if total_commented_count > 0:
            modified_content = '\n'.join(modified_lines)
            if ends_with_newline:
                modified_content += '\n'
            
            with open(sql_file_path, 'w', encoding='utf-8') as f:
                f.write(modified_content)
        
        return True

    except Exception as e:
        print(f"   [CRITICAL ERROR] An exception occurred in the core commenting engine: {e}")
        import traceback
        traceback.print_exc()
        return False
        
def _execute_commenting_safely(sql_file_path: str, unused_aliases: List[str]) -> bool:

    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        ends_with_newline = original_content.endswith('\n')
        lines = original_content.splitlines()
        modified_lines = list(lines)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unused_aliases_lower = {name.lower() for name in unused_aliases}

        if _detect_main_level_union(original_content):
            print(f"   [CORE ENGINE] UNION detected. Using symmetric strategy.")
            select_blocks = _find_all_main_select_blocks_final(lines)
            if not select_blocks: return False

            for block in select_blocks:
                for i in range(block['start'], block['end']):
                    if modified_lines[i].strip().startswith(('--', '{#', '/*')): continue
                    
                    line_alias = _get_alias_from_line_final(lines[i])
                    if line_alias and line_alias.lower() in unused_aliases_lower:
                        indentation = lines[i][:len(lines[i]) - len(lines[i].lstrip())]
                        commented_line = f"{indentation}{{# UNUSED FIELD (script {timestamp}): {lines[i].strip()} #}}"
                        modified_lines[i] = commented_line
            
            for block in select_blocks:
                _fix_commas_in_select_block(modified_lines, block['start'], block['end'])

        else:
            print(f"   [CORE ENGINE] Simple SELECT detected. Using last-block strategy.")
            safe_zone = _find_last_main_select_block(lines)
            if not safe_zone: return False

            for i in range(safe_zone['start'], safe_zone['end']):
                if modified_lines[i].strip().startswith(('--', '{#', '/*')): continue

                line_alias = _get_alias_from_line_final(lines[i])
                if line_alias and line_alias.lower() in unused_aliases_lower:
                    indentation = lines[i][:len(lines[i]) - len(lines[i].lstrip())]
                    commented_line = f"{indentation}{{# UNUSED FIELD (script {timestamp}): {lines[i].strip()} #}}"
                    modified_lines[i] = commented_line
            
            _fix_commas_in_select_block(modified_lines, safe_zone['start'], safe_zone['end'])

        final_content = "\n".join(modified_lines)
        if ends_with_newline:
            final_content += '\n'
        
        with open(sql_file_path, 'w', encoding='utf-8') as f:
            f.write(final_content)

        return True

    except Exception as e:
        print(f"   [CRITICAL CORE ENGINE ERROR] Failed to modify {os.path.basename(sql_file_path)}: {e}")
        import traceback
        traceback.print_exc()
        return False

def _fix_commas_in_select_block(modified_lines: list, start_line: int, end_line: int):
    for i in range(start_line + 1, min(end_line, len(modified_lines))):
        line = modified_lines[i]
        stripped_line = line.strip()

        if stripped_line and not stripped_line.startswith('/*') and not stripped_line.startswith('--') and not stripped_line.startswith('{#') :
            
            if stripped_line.startswith(','):
                content_without_comma = stripped_line.lstrip(',').lstrip()
                indentation = line[:len(line) - len(line.lstrip())]
                modified_lines[i] = indentation + content_without_comma
                print(f"   [COMMA FIX] Removed leading comma from line {i + 1}.")
            
            break



def _find_all_main_select_blocks(lines: List[str]) -> List[Dict[str, int]]:
    content = "\n".join(lines)
    select_line_indices = []
    
    char_count = 0
    for i, line in enumerate(lines):
        if re.search(r'^\s*select\b', line, re.IGNORECASE):
            preceding_text = content[:char_count]
            nesting_level = preceding_text.count('(') - preceding_text.count(')')
            if nesting_level == 0:
                select_line_indices.append(i)
        char_count += len(line) + 1
    
    with_match = re.search(r'^\s*with\b', content, re.IGNORECASE)
    if with_match:
        with_start_pos = with_match.start()
        
        nesting_level = 0
        end_of_with_pos = -1
        for i, char in enumerate(content[with_start_pos:]):
            if char == '(': nesting_level += 1
            elif char == ')': nesting_level -= 1
            if nesting_level == 0 and char == ')':
                end_of_with_pos = with_start_pos + i
                break
        
        if end_of_with_pos != -1:
            final_select_indices = []
            char_count = 0
            for i, line in enumerate(lines):
                if i in select_line_indices and char_count > end_of_with_pos:
                    final_select_indices.append(i)
                char_count += len(line) + 1
            select_line_indices = final_select_indices

    if not select_line_indices:
        return []

    blocks = []
    for start_index in select_line_indices:
        end_index = _find_from_line_index(lines, start_index)
        if end_index == -1:
            end_index = len(lines)
        blocks.append({'start': start_index, 'end': end_index})
        
    return blocks

def _find_from_line_index(lines: List[str], start_line: int) -> int:
    """Finds the line index of the FROM clause for a given SELECT block."""
    nesting_level = 0
    for i in range(start_line, len(lines)):
        line = lines[i]
        line_no_comments = re.sub(r'--.*', '', line)
        
        nesting_level += line_no_comments.count('(')
        nesting_level -= line_no_comments.count(')')
        
        if nesting_level == 0 and re.search(r'\bFROM\b', line, re.IGNORECASE):
            return i
            
    return -1

def _find_last_main_select_block(lines: List[str]) -> Dict[str, int]:
    """
    BULLETPROOF FINAL VERSION (Corrected). This function finds the last, top-level
    SELECT and, crucially, defines its boundary at the corresponding FROM clause,
    preventing modification of WHERE, GROUP BY, etc.
    """
    content = "\n".join(lines)
    paren_level = 0
    
    for i in range(len(content) - 1, -1, -1):
        char = content[i]
        
        if char == ')':
            paren_level += 1
        elif char == '(':
            paren_level -= 1
        
        if paren_level == 0 and content[i:i+6].upper() == 'SELECT':
            is_word_boundary_before = (i == 0) or (not content[i-1].isalnum())
            is_word_boundary_after = (i+6 >= len(content)) or (not content[i+6].isalnum())
            
            if is_word_boundary_before and is_word_boundary_after:
                start_char_pos = i
                start_line = content[:start_char_pos].count('\n')
                
                end_line = _find_from_line_index(lines, start_line)
                
                if end_line == -1:
                    end_line = len(lines)
                
                print(f"   [SAFE ZONE] Determined safe zone: lines {start_line + 1} to {end_line + 1}")
                return {'start': start_line, 'end': end_line}
    
    return None

def _detect_main_level_union(content: str) -> bool:

    union_positions = []
    for match in re.finditer(r'\bunion\s+(all\s+)?', content, re.IGNORECASE):
        union_positions.append(match.start())
    
    if not union_positions:
        return False
    
    for union_pos in union_positions:
        preceding = content[:union_pos]
        nesting_level = preceding.count('(') - preceding.count(')')
        
        if nesting_level == 0:
            return True
    
    return False



def run_commenting_out_for_table(table_name: str, unused_columns: list, tables_and_fields: list, tabular_model_path: str, dbt_models_path: str):
    global _commenting_error_log
    
    print(f"🎯 DEBUG: Starting commenting for table: {table_name}")
    print(f"   DEBUG: Unused columns count: {len(unused_columns)}")
    print(f"   DEBUG: Unused columns: {unused_columns[:3]}{'...' if len(unused_columns) > 3 else ''}")
    
    try:
        print(f"   DEBUG: Step 1 - Finding Snowflake alias...")
        snowflake_alias = find_snowflake_alias_for_table(table_name, tabular_model_path)
        print(f"   DEBUG: Snowflake alias result: '{snowflake_alias}'")
        
        if not snowflake_alias:
            error_msg = f"Could not find Snowflake alias for {table_name}"
            print(f"   ERROR: {error_msg}")
            _commenting_error_log.append({ 'table': table_name, 'error_type': 'SNOWFLAKE_ALIAS_NOT_FOUND', 'error_message': error_msg, 'columns_affected': len(unused_columns) })
            return False
        
        print(f"   DEBUG: Step 2 - Finding DBT file for alias '{snowflake_alias}'...")
        dbt_file_path = find_dbt_file_for_alias(snowflake_alias, dbt_models_path)
        print(f"   DEBUG: DBT file path: '{dbt_file_path}'")
        print(f"   DEBUG: DBT file exists: {os.path.exists(dbt_file_path) if dbt_file_path else False}")
        
        if not dbt_file_path:
            error_msg = f"Could not find DBT file for alias '{snowflake_alias}'"
            print(f"   ERROR: {error_msg}")
            _commenting_error_log.append({ 'table': table_name, 'error_type': 'DBT_FILE_NOT_FOUND', 'error_message': error_msg, 'columns_affected': len(unused_columns) })
            return False
         
        print(f"   DEBUG: Step 3 - Parsing DBT columns...")
        dbt_columns = analyze_dbt_columns_fixed(dbt_file_path)
        print(f"   DEBUG: DBT columns parsed: {len(dbt_columns)}")
        print(f"   DEBUG: First 3 DBT columns: {list(dbt_columns.items())[:3]}")
        
        if not dbt_columns:
            error_msg = f"Could not parse DBT columns for {table_name}"
            print(f"   ERROR: {error_msg}")
            _commenting_error_log.append({ 'table': table_name, 'error_type': 'DBT_PARSING_FAILED', 'error_message': error_msg, 'columns_affected': len(unused_columns) })
            return False
        
        print(f"   DEBUG: Step 4 - Preparing clean column names...")
        clean_column_names = [col.split('.', 1)[1] for col in unused_columns if '.' in col]
        print(f"   DEBUG: Clean column names: {clean_column_names[:3]}{'...' if len(clean_column_names) > 3 else ''}")
        
        print(f"   DEBUG: Step 5 - Checking column mapping...")
        field_to_sql_def = {}
        for column_name in clean_column_names:
            found_mapping = False
            for sql_def, pbi_alias in dbt_columns.items():
                if pbi_alias.lower() == column_name.lower():
                    field_to_sql_def[column_name] = sql_def
                    found_mapping = True
                    print(f"   DEBUG: Mapped '{column_name}' -> '{sql_def[:50]}...'")
                    break
            if not found_mapping:
                print(f"   DEBUG: NO MAPPING for '{column_name}' in DBT columns")
        
        print(f"   DEBUG: Total mappings found: {len(field_to_sql_def)}")
        
        if not field_to_sql_def:
            error_msg = f"No column mappings found for {table_name}"
            print(f"   ERROR: {error_msg}")
            _commenting_error_log.append({ 'table': table_name, 'error_type': 'NO_COLUMN_MAPPINGS', 'error_message': error_msg, 'columns_affected': len(unused_columns) })
            return False

        print(f"   DEBUG: Step 6 - Executing commenting...")
        success = comment_out_unused_columns_in_dbt(
            dbt_file_path, dbt_columns, clean_column_names
        )
        
        if success:
            print(f"   ✅ SUCCESS: Table {table_name} processed successfully")
            return True
        else:
            error_msg = f"Failed to comment out columns in {table_name}"
            print(f"   ERROR: {error_msg}")
            _commenting_error_log.append({ 'table': table_name, 'error_type': 'COMMENTING_FAILED', 'error_message': error_msg, 'columns_affected': len(unused_columns) })
            return False
            
    except Exception as e:
        error_msg = f"Unexpected error processing {table_name}: {str(e)}"
        print(f"   ERROR: {error_msg}")
        import traceback
        traceback.print_exc()
        _commenting_error_log.append({ 'table': table_name, 'error_type': 'UNEXPECTED_ERROR', 'error_message': error_msg, 'columns_affected': len(unused_columns) })
        return False

def run_commenting_out_for_all_tables(results: list, relationships: dict, indirect_usage: dict, tables_and_fields: list, tables_to_exclude: list, exclusion_patterns: list, user_selected_columns: list, tabular_model_path: str, dbt_models_path: str):
    global _commenting_error_log
    
    _commenting_error_log = []
    
    print(f"\n🚀 STARTING COMMENTING OUT FOR ALL TABLES")
    print("=" * 100)
    
    table_results = {}
    processed_tables = 0
    successes = 0
    excluded_tables_list = []

    for tab_config in tables_and_fields:
        table_name = tab_config["table"]
        
        is_excluded_val, reason = is_table_excluded(table_name, tables_to_exclude, exclusion_patterns)
        
        if is_excluded_val:
            print(f"↪ EXCLUDING TABLE: {table_name} (reason: {reason})")
            excluded_tables_list.append(f"{table_name} → {reason}")
            table_results[table_name] = True
            processed_tables += 1
            successes += 1
            continue
        
        columns_for_this_table = [col for col in user_selected_columns if col.startswith(f"{table_name}.")]
        
        if not columns_for_this_table:
            successes += 1
            processed_tables += 1
            continue

        success = run_commenting_out_for_table(table_name, columns_for_this_table, tables_and_fields, tabular_model_path, dbt_models_path)
        table_results[table_name] = success
        
        if success:
            successes += 1
        
        processed_tables += 1
    
    print(f"\n📊 COMMENTING OUT SUMMARY:")
    print("=" * 100)
    print(f"📢 Processed tables:    {processed_tables}")
    print(f"✅ Successes:           {successes}")
    print(f"❌ Errors:              {processed_tables - successes}")
    print(f"↪ Excluded:            {len(excluded_tables_list)}")
    
    if _commenting_error_log:
        print(f"\n🚨 ERROR DETAILS:")
        print("=" * 100)
        total_affected_columns = 0
        for error in _commenting_error_log:
            print(f"❌ Table: {error['table']}")
            print(f"   Error: {error['error_message']}")
            print(f"   Affected columns: {error['columns_affected']}")
            total_affected_columns += error['columns_affected']
            if 'snowflake_alias' in error:
                print(f"   Snowflake alias: {error['snowflake_alias']}")
            if 'dbt_file' in error:
                print(f"   DBT file: {error['dbt_file']}")
            print()
        
        print(f"⚠️ TOTAL COLUMNS NOT COMMENTED: {total_affected_columns}")
        print("💡 Check terminal log above for detailed error information")
    
    if successes > 0:
        print(f"\n🎉 Successfully processed {successes} tables!")
        print(f"📋 Check Git Changes - all changes should be visible.")
    
    return table_results


    
# ===========================
# 🚀 MAIN FUNCTION
# ===========================


def perform_analysis(zip_file_paths: List[str], tabular_model_path: str, dbt_models_path: str,
                     progress_callback=None, enable_detailed_logging=False):
    global _analysis_log_details
    _analysis_log_details = []

    def report_progress(value):
        if progress_callback: progress_callback(value)

    report_progress(0)
    log_and_print("🚀 Power BI Field Usage Analyzer - Core Logic")

    config = { "measures_folder_name": "measures", "tables_to_exclude": ["RefreshDate"], "exclusion_patterns": ["partition", "refresh"] }
    for zip_path in zip_file_paths:
        if not Path(zip_path).exists(): raise FileNotFoundError(f"PBIX file not found: {zip_path}")

    report_progress(5)
    log_and_print("\n📋 STEP 1: Dynamically loading columns from the model...")
    tables_and_fields = dynamically_generate_field_config(tabular_model_path, config["tables_to_exclude"], config["exclusion_patterns"], config["measures_folder_name"])
    if not tables_and_fields: raise ValueError("Failed to load any tables from the model.")
    all_fields = [f"{conf['table']}.{field}" for conf in tables_and_fields for field in conf['fields']]

    log_and_print("📋 STEP 1.5: Searching for usage in 'Sort By Column'...")
    sort_by_columns = find_usage_in_sort_by_column(tables_and_fields, tabular_model_path)

    log_and_print("📋 STEP 1.6: Searching for usage in RLS (Row-Level Security)...")
    rls_columns = find_usage_in_rls_filters(tabular_model_path)

    report_progress(20)
    log_and_print("📋 STEP 2: Searching for direct field usage in PBIX...")
    direct_usage = search_for_field_usage(zip_file_paths, tables_and_fields, detailed_logging=enable_detailed_logging)

    report_progress(50)
    log_and_print("📋 STEP 3: Loading and analyzing measures...")
    # ... (loading measures without change)
    measures_path = ""
    if Path(tabular_model_path).exists():
        for folder in glob.glob(os.path.join(tabular_model_path, '**', '*'), recursive=True):
            if os.path.isdir(folder) and config["measures_folder_name"].lower() in os.path.basename(folder).lower():
                measures_path = folder
                break
    measure_defs = load_measures_from_tabular_editor(measures_path, tables_and_fields, tabular_model_path)
    dependencies = analyze_measure_dependencies(measure_defs, all_fields, detailed_logging=enable_detailed_logging)
    basic_dependencies = dependencies['basic_dependencies'] if isinstance(dependencies, dict) and 'basic_dependencies' in dependencies else dependencies
    indirect_usage = find_indirect_usage_by_measures(direct_usage, basic_dependencies, all_fields)

    report_progress(75)
    log_and_print("📋 STEP 4: Checking relationships...")
    relationships = search_for_relationships(tabular_model_path, all_fields)

    report_progress(90)
    log_and_print("📋 STEP 5: Preparing initial results for UI...")
    ui_results = []
    for table_config in tables_and_fields:
        table_name, fields_list, hierarchy_fields = table_config.get("table"), table_config.get("fields", []), table_config.get("fields_in_hierarchies", [])
        if not table_name or not fields_list: continue
        for field in fields_list:
            is_excluded, _ = is_field_excluded(field, config["exclusion_patterns"])
            if is_excluded: continue

            full_name = f"{table_name}.{field}"
            ui_results.append({
                "table": table_name, "column": field,
                "visualization": any(res.get('field') == full_name and 'VISUALIZATION' in res.get('usage_type', '') for res in direct_usage),
                "measure": any(res.get('field') == full_name and 'MEASURE' in res.get('usage_type', '') for res in direct_usage),
                "indirect_measure": full_name in indirect_usage,
                "hierarchy": field in hierarchy_fields,
                "filter": any(res.get('field') == full_name and 'FILTER' in res.get('usage_type', '') for res in direct_usage),
                "relationship": relationships.get(full_name, False),
                "tabular_sort": full_name in sort_by_columns,
                "rls": full_name in rls_columns
            })

    # ==============================================================================
    # 🚀 STEP 5.5: NEW LOGIC - Validating hidden intra-file dependencies
    # ==============================================================================
    log_and_print("📋 STEP 5.5: Checking for hidden intra-file dependencies...")

    # Create a map for quick field usage status checks
    usage_status_map = {f"{item['table']}.{item['column']}": any(item.values()) for item in ui_results}

    # Group fields by table (file)
    results_by_table = {}
    for item in ui_results:
        if item['table'] not in results_by_table: results_by_table[item['table']] = []
        results_by_table[item['table']].append(item)

    final_ui_results = []
    for table_name, fields_in_table in results_by_table.items():
        try:
            alias = find_snowflake_alias_for_table(table_name, tabular_model_path)
            dbt_file = find_dbt_file_for_alias(alias, dbt_models_path) if alias else None
            if not dbt_file or not os.path.exists(dbt_file):
                final_ui_results.extend(fields_in_table)
                continue

            with open(dbt_file, 'r', encoding='utf-8') as f: content = f.read()

            for field_A in fields_in_table:
                field_A_fullname = f"{field_A['table']}.{field_A['column']}"

                # Run logic only for fields that are initially unused
                if not usage_status_map.get(field_A_fullname, True):
                    # Count occurrences
                    occurrences = len(re.findall(r'(?<![\w\d_])' + re.escape(field_A['column']) + r'(?![\w\d_])', content, re.IGNORECASE))

                    if occurrences > 1:
                        # "Investigation mode"
                        is_blocked_by_neighbor = False
                        for field_B in fields_in_table:
                            if field_A == field_B: continue

                            # Find the definition line of the neighbor (field_B)
                            for line in content.splitlines():
                                defined_alias = _get_alias_from_line_final(line)
                                if defined_alias and defined_alias.lower() == field_B['column'].lower():
                                    # Check if our field (field_A) is in the neighbor's definition (field_B)
                                    if re.search(r'(?<![\w\d_])' + re.escape(field_A['column']) + r'(?![\w\d_])', line, re.IGNORECASE):
                                        # It is used. Check the neighbor's status.
                                        field_B_fullname = f"{field_B['table']}.{field_B['column']}"
                                        if usage_status_map.get(field_B_fullname, False):
                                            # Neighbor is used, so we block it
                                            log_and_print(f"   -> BLOCKED: Field '{field_A_fullname}' is a component of the used field '{field_B_fullname}'.")
                                            field_A['relationship'] = True # Let's use the 'relationship' field as a blocking flag
                                            is_blocked_by_neighbor = True
                                            break
                            if is_blocked_by_neighbor: break

                final_ui_results.append(field_A)
        except Exception as e:
            log_and_print(f"   -> WARNING: Error during intra-file analysis for table '{table_name}': {e}")
            final_ui_results.extend(fields_in_table)

    intermediate_data = {
        "direct_usage": direct_usage, "relationships": relationships, "indirect_usage": indirect_usage,
        "tables_and_fields": tables_and_fields, "config": config,
        "dbt_models_path": dbt_models_path, "tabular_model_path": tabular_model_path
    }

    report_progress(100)
    log_and_print(f"✅ Analysis complete. Prepared {len(final_ui_results)} rows for the UI.")

    return final_ui_results, intermediate_data

def apply_changes(dbt_path: str, intermediate_data: dict):
    print("\n🎯 Applying Changes...")
    
    columns_to_comment_out = intermediate_data.get('columns_to_comment_out', [])
    if not columns_to_comment_out:
        print("No columns were selected for commenting out.")
        return

    print(f"Will process {len(columns_to_comment_out)} columns selected by the user.")
    
    config = intermediate_data["config"]
    exclusion_patterns = config["exclusion_patterns"]
    
    filtered_columns = []
    excluded_count = 0
    
    for column in columns_to_comment_out:
        if '.' in column:
            table_name, field_name = column.split('.', 1)
            
            is_excluded, reason = is_field_excluded(field_name, exclusion_patterns)
            if is_excluded:
                print(f"🔍 Skipping excluded field during commenting: '{column}' (reason: {reason})")
                excluded_count += 1
                continue
            
            is_table_excluded_val, table_reason = is_table_excluded(table_name, config["tables_to_exclude"], exclusion_patterns)
            if is_table_excluded_val:
                print(f"🔍 Skipping field from excluded table: '{column}' (reason: {table_reason})")
                excluded_count += 1
                continue
                
            filtered_columns.append(column)
        else:
            filtered_columns.append(column)
    
    print(f"After filtering: {len(filtered_columns)} columns will be processed ({excluded_count} excluded)")
    
    if not filtered_columns:
        print("No columns remaining after filtering for commenting out.")
        return

    run_commenting_out_for_all_tables(
        results=intermediate_data["direct_usage"],
        relationships=intermediate_data["relationships"],
        indirect_usage=intermediate_data["indirect_usage"],
        tables_and_fields=intermediate_data["tables_and_fields"],
        tables_to_exclude=intermediate_data["config"]["tables_to_exclude"],
        exclusion_patterns=intermediate_data["config"]["exclusion_patterns"],
        user_selected_columns=filtered_columns,  # Używamy przefiltrowanej listy
        tabular_model_path=intermediate_data["tabular_model_path"],
        dbt_models_path=dbt_path
    )
    print("✅ Changes applied.")

def _calculate_dax_complexity(dax_expression: str) -> int:
    if not dax_expression:
        return 0
    
    complexity = 0

    dax_functions = ['CALCULATE', 'FILTER', 'ALL', 'ALLEXCEPT', 'SUMX', 'COUNTX', 'EARLIER', 'RELATED']
    for func in dax_functions:
        complexity += dax_expression.upper().count(func) * 2

    complexity += dax_expression.count('(')

    complexity += len(dax_expression) // 100
    
    return complexity

def _calculate_dependency_depth(measure_name: str, dependencies: Dict[str, set], 
                              visited: set = None, depth: int = 0) -> int:
    if visited is None:
        visited = set()
    
    if measure_name in visited:
        return depth  # Avoid infinite recursion
    
    visited.add(measure_name)
    max_depth = depth
    
    for dep in dependencies.get(measure_name, set()):
        if dep.startswith('MEASURE:'):
            dep_measure = dep[8:]  # Remove 'MEASURE:' prefix
            child_depth = _calculate_dependency_depth(dep_measure, dependencies, visited.copy(), depth + 1)
            max_depth = max(max_depth, child_depth)
    
    return max_depth

def _find_most_referenced_fields(dependencies: Dict[str, set]) -> List[tuple]:
    field_counts = {}
    
    for measure_deps in dependencies.values():
        for dep in measure_deps:
            if not dep.startswith('MEASURE:'):
                field_counts[dep] = field_counts.get(dep, 0) + 1

    return sorted(field_counts.items(), key=lambda x: x[1], reverse=True)[:10]

def extract_usage_context_simple(json_data: Any, field_name: str, table_name: str) -> str:
    try:
        if not isinstance(json_data, dict) or not field_name:
            return 'Invalid Input'

        json_str = json.dumps(json_data).lower()
        field_lower = field_name.lower()

        context_patterns = {
            'legend': ['legend', 'colorvalue', 'series'],
            'values': ['values', 'measure', 'sum(', 'count('],
            'filter': ['filter', 'slicer', 'where'],
            'axis': ['axis', 'category', 'column', 'row']
        }

        for context_type, patterns in context_patterns.items():
            for pattern in patterns:
                if pattern in json_str and field_lower in json_str:
                    pattern_pos = json_str.find(pattern)
                    field_pos = json_str.find(field_lower)
                    
                    if pattern_pos != -1 and field_pos != -1:
                        distance = abs(pattern_pos - field_pos)
                        if distance < 100:
                            return context_type.title()
        
        return 'Unknown'
        
    except Exception as e:
        return f'Error: {str(e)[:20]}'

    
def generate_error_report() -> dict:
    global _commenting_error_log
    
    if not _commenting_error_log:
        return {'has_errors': False}

    error_types = {}
    total_affected_columns = 0
    failed_tables = []
    
    for error in _commenting_error_log:
        error_type = error['error_type']
        if error_type not in error_types:
            error_types[error_type] = 0
        error_types[error_type] += 1
        total_affected_columns += error['columns_affected']
        failed_tables.append(error['table'])

    error_descriptions = {
        'SNOWFLAKE_ALIAS_NOT_FOUND': 'Cannot find Snowflake table alias in model',
        'DBT_FILE_NOT_FOUND': 'Cannot find corresponding DBT file',
        'DBT_PARSING_FAILED': 'Cannot parse columns from DBT file',
        'COMMENTING_FAILED': 'Failed to comment out columns in DBT file',
        'UNEXPECTED_ERROR': 'Unexpected error during processing'
    }
    
    return {
        'has_errors': True,
        'total_errors': len(_commenting_error_log),
        'total_affected_columns': total_affected_columns,
        'failed_tables': failed_tables,
        'error_types': error_types,
        'error_descriptions': error_descriptions,
        'detailed_errors': _commenting_error_log
    }

def get_marts_path_from_reporting(reporting_path: str) -> str:
    if 'reporting' not in reporting_path:
        return ""
    
    marts_path = reporting_path.replace('reporting', 'marts')
    if not os.path.exists(marts_path):
        print(f"⚠️ Marts path does not exist: {marts_path}")
        return ""
    
    return marts_path

def can_comment_field_in_marts_final(field_to_check: str, source_model: str, scan_path: str, file_to_ignore: str = None) -> tuple[bool, list]:
    blocking_info = []
    source_model_variants = [source_model, source_model.replace('marts_', '')]
    sql_files = glob.glob(os.path.join(scan_path, "**/*.sql"), recursive=True)

    for sql_file_path in sql_files:
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                original_content = f.read()

            file_name_without_ext = os.path.splitext(os.path.basename(sql_file_path))[0]
            is_self = file_name_without_ext in source_model_variants

            if file_to_ignore and os.path.basename(sql_file_path) == file_to_ignore:
                continue

            if is_self:
                content_no_comments = re.sub(r'--.*', '', original_content)
                lines = content_no_comments.splitlines()
                is_blocked_internally = False
                for i, line in enumerate(lines):
                    if not line.strip(): continue
                    defined_alias = _get_alias_from_line_final(line)
                    if defined_alias and defined_alias.lower() == field_to_check.lower():
                        continue 

                    if re.search(r'\b' + re.escape(field_to_check) + r'\b', line, re.IGNORECASE):
                        blocking_info.append({
                            'file': os.path.basename(sql_file_path),
                            'context': f"Blocked by internal dependency on line {i+1}."
                        })
                        is_blocked_internally = True
                        break 
                if is_blocked_internally:
                    continue
            
            is_referenced = False
            for v in source_model_variants:
                ref_pattern = rf"ref\s*\(\s*['\"]{re.escape(v)}['\"]\s*\)"
                if re.search(ref_pattern, original_content, re.IGNORECASE | re.DOTALL):
                    is_referenced = True
                    break

            if is_referenced:
                content_no_comments = re.sub(r'--.*', '', original_content)

                if re.search(r'\b' + re.escape(field_to_check) + r'\b', content_no_comments, re.IGNORECASE):
                    blocking_info.append({
                        'file': os.path.basename(sql_file_path),
                        'context': f"Usage of field '{field_to_check}' found in a dependent model."
                    })
                    continue

        except Exception as e:
            blocking_info.append({'file': os.path.basename(sql_file_path), 'context': f"Error during analysis: {e}"})

    can_comment = len(blocking_info) == 0
    return can_comment, blocking_info


def analyze_marts_optimization(reporting_path: str, tabular_model_path: str, 
                              commented_fields_in_reporting: list, progress_callback=None) -> dict:

    marts_path = reporting_path.replace('reporting', 'marts')
    if not os.path.exists(marts_path):
        raise ValueError(f"Marts path could not be found. Expected at: {marts_path}")

    results = {
        'marts_path': marts_path,
        'can_comment_in_marts': [],
        'cannot_comment_in_marts': [],
        'errors': [],
        'summary': {}
    }
    
    total_fields = len(commented_fields_in_reporting)
    if total_fields == 0: return results

    for i, field_full in enumerate(commented_fields_in_reporting):
        if progress_callback:
            progress_callback(int(((i + 1) / total_fields) * 100))

        if not isinstance(field_full, str) or '.' not in field_full:
            results['errors'].append({'field': str(field_full), 'error': 'Invalid data format'})
            continue
        
        table_name_from_pbi, field_name = field_full.split('.', 1)
        
        alias = find_snowflake_alias_for_table(table_name_from_pbi, tabular_model_path)
        if not alias:
            results['errors'].append({'field': field_full, 'error': f"Could not find Snowflake alias for '{table_name_from_pbi}'"})
            continue

        reporting_sql_file_initiator = find_dbt_file_for_alias(alias, reporting_path)
        if not reporting_sql_file_initiator:
            results['errors'].append({'field': field_full, 'error': f"Reporting SQL for alias '{alias}' not found"})
            continue

        source_marts_model_name = find_source_marts_model_from_reporting_file(reporting_sql_file_initiator)
        if not source_marts_model_name:
            results['errors'].append({'field': field_full, 'error': f"Source MARTS model ref not found in '{os.path.basename(reporting_sql_file_initiator)}'"})
            continue

        marts_sql_file = find_dbt_file_for_alias(source_marts_model_name, marts_path)
        can_comment_marts, blocking_details_marts = can_comment_field_in_marts_final(
            field_name, 
            source_marts_model_name, 
            marts_path,
            file_to_ignore=os.path.basename(marts_sql_file) if marts_sql_file else None
        )

        can_comment_reporting, blocking_details_reporting = can_comment_field_in_marts_final(
            field_name, 
            source_marts_model_name, 
            reporting_path,
            file_to_ignore=os.path.basename(reporting_sql_file_initiator)
        )

        if can_comment_marts and can_comment_reporting:
            results['can_comment_in_marts'].append({
                'field': field_full,
                'source_model': source_marts_model_name
            })
        else:
            all_blocking_details = blocking_details_marts + blocking_details_reporting
            blocking_files = list(set([info['file'] for info in all_blocking_details]))
            full_blocking_context = [f"{info['file']}: {info['context']}" for info in all_blocking_details]
            
            results['cannot_comment_in_marts'].append({
                'field': field_full,
                'source_model': source_marts_model_name,
                'blocking_models': blocking_files,
                'blocking_details': full_blocking_context,
            })

    results['summary'] = {
        'can_optimize': len(results['can_comment_in_marts']),
        'blocked': len(results['cannot_comment_in_marts']),
        'errors': len(results['errors']),
    }
    return results
   
def _find_all_main_select_blocks_final(lines: List[str]) -> List[Dict[str, int]]:
    """
    ROBUST AND SIMPLIFIED FINAL VERSION. This function correctly identifies all
    top-level SELECT statements, making it resilient to complex CTE structures
    like WITH RECURSIVE and UNION ALL.
    """
    content = "\n".join(lines)
    select_line_indices = []

    char_count = 0
    for i, line in enumerate(lines):
        if re.search(r'^\s*SELECT\b', line, re.IGNORECASE):
            preceding_text = content[:char_count]
            nesting_level = preceding_text.count('(') - preceding_text.count(')')
            
            if nesting_level == 0:
                select_line_indices.append(i)
        char_count += len(line) + 1

    if not select_line_indices:
        return []

    blocks = []
    for i, start_index in enumerate(select_line_indices):
        if i + 1 < len(select_line_indices):
            end_index = select_line_indices[i + 1]
        else:
            end_index = len(lines)

        from_line_index = _find_from_line_index(lines, start_index)

        if from_line_index == -1 or from_line_index >= end_index:
             final_end_line = end_index
        else:
             final_end_line = from_line_index + 1
        
        blocks.append({'start': start_index, 'end': final_end_line})
        
    return blocks

def _get_alias_from_line_final(line: str) -> str:
    cleaned_line = re.sub(r'/\*.*?\*/', '', line)
    cleaned_line = re.sub(r'--.*', '', cleaned_line).strip()

    if not cleaned_line:
        return None

    upper_line = cleaned_line.upper()
    as_marker = ' AS '
    as_pos = upper_line.rfind(as_marker)

    if as_pos != -1:
        alias_part = cleaned_line[as_pos + len(as_marker):]
        return alias_part.strip().strip(',').strip('"')

    parts = cleaned_line.split()
    if not parts:
        return None
    
    last_word = parts[-1]
    return last_word.split('.')[-1].strip('",')

def _is_field_definition_simple(sql_content: str, field_name: str) -> bool:
    """
    Checks if a field's definition is simple (a direct rename) or complex (a calculation).
    This improved version correctly handles simple aliases.
    """
    for line in sql_content.splitlines():
        defined_alias = _get_alias_from_line_final(line)

        if defined_alias and defined_alias.lower() == field_name.lower():

            if not re.search(r'\bAS\b', line, re.IGNORECASE):
                return True

            expression_part = line.split(re.search(r'AS\b', line, re.IGNORECASE).group(0))[0].strip().rstrip(',')

            if len(expression_part.split()) > 1:
                return False
            else:
                return True

    return False


def _get_zone_of_interest(lines: List[str]) -> set:
    main_select_blocks = _find_all_main_select_blocks_final(lines)
    if not main_select_blocks:
        return set()
    first_select_start_line = main_select_blocks[0]['start']
    return set(range(first_select_start_line, len(lines)))

def comment_out_unused_columns_in_dbt(sql_file_path: str, dbt_columns: Dict[str, str], unused_column_names: List[str]) -> bool:
    print(f"   [REPORTING WRAPPER] Delegating modification of {os.path.basename(sql_file_path)} to core engine.")
    
    return _execute_commenting_safely(sql_file_path, unused_column_names)

def comment_out_fields_in_marts(marts_analysis_results: dict, tabular_model_path: str, reporting_path: str) -> dict:
    fields_to_process = marts_analysis_results.get('can_comment_in_marts', [])
    if not fields_to_process:
        return {'commented_count': 0, 'failed_count': 0, 'summary': 'No fields to comment.'}

    fields_by_marts_model = {}
    for field_info in fields_to_process:
        source_model = field_info.get('source_model')
        if not source_model:
            continue

        if source_model not in fields_by_marts_model:
            fields_by_marts_model[source_model] = []

        field_name = field_info['field'].split('.', 1)[-1]
        fields_by_marts_model[source_model].append(field_name)

    commented_count = 0
    failed_count = 0
    marts_path = marts_analysis_results['marts_path']

    for source_marts_model, field_aliases in fields_by_marts_model.items():
        print(f"\n[MARTS WRAPPER] Processing model: {source_marts_model}")
        marts_sql_file = find_dbt_file_for_alias(source_marts_model, marts_path)
        
        if not marts_sql_file:
            print(f"   [ERROR] SQL file not found for '{source_marts_model}'. Skipping.")
            failed_count += len(field_aliases)
            continue
        
        print(f"   [MARTS WRAPPER] Delegating modification of {os.path.basename(marts_sql_file)} to core engine.")
        success = _execute_commenting_safely(marts_sql_file, field_aliases)
        
        if success:
            commented_count += len(field_aliases)
        else:
            failed_count += len(field_aliases)

    return {
        'commented_count': commented_count,
        'failed_count': failed_count,
        'summary': f"Processed {commented_count + failed_count} fields. Success: {commented_count}, Failed: {failed_count}."
    }


def _extract_column_alias_for_audit(line_for_parsing: str) -> str:
    """
    ISOLATED & IMPROVED: Extracts a column's alias, avoiding SQL keywords.
    This version is for the MARTS AUDIT functionality only.
    """
    SQL_KEYWORDS = {
        'select', 'from', 'where', 'join', 'left', 'right', 'inner', 'outer', 'on', 'group', 'by', 
        'order', 'having', 'as', 'case', 'when', 'then', 'else', 'end', 'and', 'or', 'not',
        'distinct', 'over', 'partition', 'union', 'all', 'limit', 'offset', 'fetch',
        'cast', 'convert', 'try_cast', 'try_convert',
        'in', 'like', 'between', 'is', 'null', 'not', 'exists', 'coalesce', 'ifnull', 'nullif',
        'sum', 'count', 'avg', 'min', 'max', 'listagg', 'string_agg',
        'rank', 'dense_rank', 'row_number', 'lag', 'lead', 'first_value', 'last_value',
        'date', 'timestamp', 'date_trunc', 'dateadd', 'datediff', 'year', 'month', 'day',
        'current_date', 'current_timestamp',
        'concat', 'substring', 'left', 'right', 'len', 'length', 'trim', 'lower', 'upper',
        'pivot', 'unpivot',
        'with', 'values', 'true', 'false', 'using'
    }

    cleaned_line = re.sub(r'--.*', '', line_for_parsing).strip().replace('\n', ' ').replace('\r', ' ')
    
    as_match = re.search(r'\s+AS\s+("?[\w\d_]+"?|`?[\w\d_]+`?)\s*$', cleaned_line, re.IGNORECASE)
    if as_match:
        return as_match.group(1).strip('"`')

    parts = cleaned_line.split()
    if not parts:
        return ""
    potential_alias = parts[-1].strip('",`')
    
    if (potential_alias.lower() not in SQL_KEYWORDS and
            not potential_alias.endswith('_') and
            re.search(r'[a-zA-Z]', potential_alias)):
        
        if '.' in parts[-1]:
             return potential_alias.split('.')[-1].strip('"`')
        return potential_alias
    return ""

def analyze_dbt_columns_for_audit(file_path: str) -> Dict[str, str]:
    if not os.path.exists(file_path):
        return {}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
        main_select_content = _find_main_select_by_patterns(content)
        if not main_select_content:
            return {}
        
        column_definitions = _parse_column_definitions(main_select_content)
        dbt_columns = OrderedDict()
        for col_def in column_definitions:
            line_for_parsing = re.sub(r'--.*', '', col_def).replace('\n', ' ').replace('\r', ' ')
            line_for_parsing = ' '.join(line_for_parsing.split())
            # USE THE NEW, ISOLATED ALIAS EXTRACTOR
            alias = _extract_column_alias_for_audit(line_for_parsing)
            if alias:
                dbt_columns[col_def] = alias
        return dbt_columns
    except Exception as e:
        print(f"   [MARTS AUDIT] Error analyzing DBT columns in '{file_path}': {e}")
        return {}

def get_all_fields_from_dbt_path_for_audit(path: str) -> List[Dict]:
    """
    ISOLATED: A copy of get_all_fields_from_dbt_path that uses the improved column parser.
    This version is for the MARTS AUDIT functionality only.
    """
    if not os.path.exists(path):
        print(f"⚠️ [Marts Audit Collector] Path does not exist: {path}")
        return []

    all_fields = []
    sql_files = glob.glob(os.path.join(path, "**", "*.sql"), recursive=True)
    
    print(f"🔍 [Marts Audit Collector] Found {len(sql_files)} .sql files in '{path}' to scan.")

    for file_path in sql_files:
        try:
            model_name = os.path.splitext(os.path.basename(file_path))[0]
            # USE THE NEW, ISOLATED COLUMN ANALYZER
            dbt_columns = analyze_dbt_columns_for_audit(file_path)
            
            for pbi_alias in dbt_columns.values():
                all_fields.append({
                    "field": pbi_alias,
                    "source_model": model_name
                })
        except Exception as e:
            print(f"⚠️ [Marts Audit Collector] Error while processing file {os.path.basename(file_path)}: {e}")
            continue
            
    print(f"✅ [Marts Audit Collector] Successfully collected {len(all_fields)} fields.")
    return all_fields

def _extract_column_alias_for_audit(line_for_parsing: str) -> str:
    """
    ISOLATED & IMPROVED: Extracts a column's alias, avoiding SQL keywords.
    This version is for the MARTS AUDIT functionality only.
    """
    SQL_KEYWORDS = {
        'select', 'from', 'where', 'join', 'left', 'right', 'inner', 'outer', 'on', 'group', 'by', 
        'order', 'having', 'as', 'case', 'when', 'then', 'else', 'end', 'and', 'or', 'not',
        'distinct', 'over', 'partition', 'union', 'all', 'limit', 'offset', 'fetch',
        'cast', 'convert', 'try_cast', 'try_convert',
        'in', 'like', 'between', 'is', 'null', 'not', 'exists', 'coalesce', 'ifnull', 'nullif',
        'sum', 'count', 'avg', 'min', 'max', 'listagg', 'string_agg',
        'rank', 'dense_rank', 'row_number', 'lag', 'lead', 'first_value', 'last_value',      
        'date', 'timestamp', 'date_trunc', 'dateadd', 'datediff', 'year', 'month', 'day',
        'current_date', 'current_timestamp',
        'concat', 'substring', 'left', 'right', 'len', 'length', 'trim', 'lower', 'upper',
        'pivot', 'unpivot',
        'with', 'values', 'true', 'false', 'using'
    }

    cleaned_line = re.sub(r'--.*', '', line_for_parsing).strip().replace('\n', ' ').replace('\r', ' ')
    
    as_match = re.search(r'\s+AS\s+("?[\w\d_]+"?|`?[\w\d_]+`?)\s*$', cleaned_line, re.IGNORECASE)
    if as_match:
        return as_match.group(1).strip('"`')

    parts = cleaned_line.split()
    if not parts:
        return ""
    potential_alias = parts[-1].strip('",`')
    
    if (potential_alias.lower() not in SQL_KEYWORDS and
            not potential_alias.endswith('_') and
            re.search(r'[a-zA-Z]', potential_alias)):
        
        if '.' in parts[-1]:
             return potential_alias.split('.')[-1].strip('"`')
        return potential_alias
    return ""

def analyze_dbt_columns_for_audit(file_path: str) -> Dict[str, str]:
    """
    ISOLATED: A copy of analyze_dbt_columns_fixed that uses the improved alias extraction.
    This version is for the MARTS AUDIT functionality only.
    """
    if not os.path.exists(file_path):
        return {}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
        main_select_content = _find_main_select_by_patterns(content)
        if not main_select_content:
            return {}
        
        column_definitions = _parse_column_definitions(main_select_content)
        dbt_columns = OrderedDict()
        for col_def in column_definitions:
            line_for_parsing = re.sub(r'--.*', '', col_def).replace('\n', ' ').replace('\r', ' ')
            line_for_parsing = ' '.join(line_for_parsing.split())
            # USE THE NEW, ISOLATED ALIAS EXTRACTOR
            alias = _extract_column_alias_for_audit(line_for_parsing)
            if alias:
                dbt_columns[col_def] = alias
        return dbt_columns
    except Exception as e:
        print(f"   [MARTS AUDIT] Error analyzing DBT columns in '{file_path}': {e}")
        return {}

def get_all_fields_from_dbt_path_for_audit(path: str) -> List[Dict]:
    """
    ISOLATED: A copy of get_all_fields_from_dbt_path that uses the improved column parser.
    This version is for the MARTS AUDIT functionality only.
    """
    if not os.path.exists(path):
        print(f"⚠️ [Marts Audit Collector] Path does not exist: {path}")
        return []

    all_fields = []
    sql_files = glob.glob(os.path.join(path, "**", "*.sql"), recursive=True)
    
    print(f"🔍 [Marts Audit Collector] Found {len(sql_files)} .sql files in '{path}' to scan.")

    for file_path in sql_files:
        try:
            model_name = os.path.splitext(os.path.basename(file_path))[0]
            # USE THE NEW, ISOLATED COLUMN ANALYZER
            dbt_columns = analyze_dbt_columns_for_audit(file_path)
            
            for pbi_alias in dbt_columns.values():
                all_fields.append({
                    "field": pbi_alias,
                    "source_model": model_name
                })
        except Exception as e:
            print(f"⚠️ [Marts Audit Collector] Error while processing file {os.path.basename(file_path)}: {e}")
            continue
            
    print(f"✅ [Marts Audit Collector] Successfully collected {len(all_fields)} fields.")
    return all_fields
