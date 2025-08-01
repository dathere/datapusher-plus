"""
Enhanced Vector Store integration for DataPusher Plus
"""
import os
import json
import logging
import traceback
import requests
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from pinecone import Pinecone
import ckanext.datapusher_plus.config as conf

log = logging.getLogger(__name__)


class DataPusherVectorStore:
    """Enhanced vector store for embedding resources with comprehensive QSV analysis"""
    
    def __init__(self):
        self.enabled = conf.ENABLE_VECTOR_STORE
        if not self.enabled:
            log.info("Vector store embedding is disabled")
            return
            
        self.index_name = conf.VECTOR_STORE_INDEX_NAME
        self.namespace = conf.VECTOR_STORE_NAMESPACE
        
        # Initialize components
        self._initialize_components()
        
    def _initialize_components(self):
        """Initialize vector store components"""
        try:
            # Initialize Pinecone
            self.pc = Pinecone(api_key=conf.PINECONE_API_KEY)
            self.index = self.pc.Index(self.index_name)
            
            log.info(f"Connected to Pinecone index '{self.index_name}'")
            
            # Initialize OpenRouter configuration
            self.openrouter_api_key = conf.OPENROUTER_API_KEY
            self.openrouter_model = conf.OPENROUTER_MODEL
            self.openrouter_base_url = conf.OPENROUTER_BASE_URL
            
            log.info("Enhanced vector store components initialized successfully") 
            
        except Exception as e:
            log.error(f"Failed to initialize vector store: {e}")
            self.enabled = False
    
    def _safe_int(self, value, default=0):
        """Safely convert value to int with fallback"""
        try:
            return int(value) if value is not None else default
        except (ValueError, TypeError):
            return default
    
    def _safe_float(self, value, default=0.0):
        """Safely convert value to float with fallback"""
        try:
            return float(value) if value is not None else default
        except (ValueError, TypeError):
            return default
    
    def _call_openrouter(self, prompt: str, system_prompt: str = None, 
                        temperature: float = None, max_tokens: int = None) -> str:
        """Call OpenRouter API for text generation"""
        if temperature is None:
            temperature = conf.VECTOR_AI_TEMPERATURE
        if max_tokens is None:
            max_tokens = conf.VECTOR_AI_MAX_TOKENS
        
        # Validate API key
        if not self.openrouter_api_key:
            log.error("OpenRouter API key is not configured")
            return ""
            
        headers = {
            "Authorization": f"Bearer {self.openrouter_api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/dathere/datapusher-plus",
            "X-Title": "DataPusher Plus Vector Store"
        }
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        data = {
            "model": self.openrouter_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        log.debug(f"OpenRouter request: model={self.openrouter_model}, temp={temperature}, max_tokens={max_tokens}")
        log.debug(f"Prompt length: {len(prompt)} characters")
        
        try:
            log.debug("Calling OpenRouter API...")
            response = requests.post(
                f"{self.openrouter_base_url}/chat/completions",
                headers=headers,
                json=data,
                timeout=conf.VECTOR_TIMEOUT
            )
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    content = result.get('choices', [{}])[0].get('message', {}).get('content', '')
                    if content:
                        log.debug(f"OpenRouter response received: {len(content)} characters")
                        return content.strip()
                    else:
                        log.warning("OpenRouter returned empty content in response")
                        log.debug(f"Full response structure: {result}")
                        return ""
                except json.JSONDecodeError as e:
                    log.error(f"Failed to parse OpenRouter JSON response: {e}")
                    log.debug(f"Response text: {response.text[:500]}")
                    return ""
            else:
                log.error(f"OpenRouter API error {response.status_code}: {response.text[:500]}")
                return ""
                
        except requests.exceptions.Timeout:
            log.error(f"OpenRouter API timeout after {conf.VECTOR_TIMEOUT} seconds")
            return ""
        except requests.exceptions.RequestException as e:
            log.error(f"OpenRouter API request error: {e}")
            return ""
        except Exception as e:
            log.error(f"Error calling OpenRouter: {e}")
            return ""

    def embed_resource(self, 
                      resource_id: str,
                      resource_metadata: Dict[str, Any],
                      dataset_metadata: Dict[str, Any],
                      stats_data: Dict[str, Any],
                      freq_data: Dict[str, Any],
                      temporal_info: Optional[Dict[str, Any]] = None,
                      sample_data: Optional[str] = None,
                      logger=None) -> bool:
        """Embed a resource after successful datastore upload - optimized for single chunk per resource"""
        
        if not self.enabled:
            return False
            
        if logger is None:
            logger = log
            
        try:
            logger.info(f"Starting enhanced vector embedding for resource {resource_id}")
            
            # Create resource profile from available data
            logger.debug("Creating resource profile...")
            profile = self._create_resource_profile(
                resource_id, resource_metadata, dataset_metadata, 
                stats_data, freq_data, temporal_info
            )
            logger.debug("Resource profile created successfully")
            
            # Generate comprehensive AI description and tags using FULL QSV data
            if conf.VECTOR_INCLUDE_AI_DESCRIPTION:
                logger.debug("Generating AI description with full QSV analysis...")
                try:
                    ai_description, ai_tags = self._generate_ai_description_with_full_data(
                        profile, stats_data, freq_data, sample_data
                    )
                    profile['ai_description'] = ai_description
                    profile['ai_tags'] = ai_tags if conf.VECTOR_INCLUDE_AI_TAGS else []
                    
                    logger.info(f"AI analysis complete: {len(ai_tags)} tags generated")
                    logger.debug(f"AI tags: {', '.join(ai_tags[:5])}...")
                    
                except Exception as e:
                    logger.warning(f"Failed to generate AI description: {e}")
                    if conf.VECTOR_FALLBACK_ON_AI_FAILURE:
                        profile['ai_description'] = f"Resource containing {profile['resource_name']} data"
                        profile['ai_tags'] = []
                    else:
                        return False
            
            # Generate detailed analysis from QSV data
            if conf.VECTOR_INCLUDE_COLUMN_ANALYSIS:
                profile['column_analysis'] = self._analyze_columns_from_qsv(stats_data, freq_data)
            
            if conf.VECTOR_INCLUDE_FREQUENCY_INSIGHTS:
                profile['frequency_insights'] = self._extract_frequency_insights(freq_data)
            
            # Create SINGLE comprehensive document content (no chunking)
            logger.debug("Creating comprehensive document content...")
            doc_content = self._create_enhanced_document_content(profile)
            logger.info(f"Single document created, length: {len(doc_content)} characters")
            
            # Create enhanced metadata for filtering
            logger.debug("Creating enhanced metadata...")
            metadata = self._create_enhanced_metadata(profile)
            logger.debug(f"Enhanced metadata created with {len(metadata)} fields")
            
            # Remove existing entries for this resource
            logger.debug("Checking for existing entries to delete...")
            try:
                self.index.delete(
                    filter={"resource_id": resource_id},
                    namespace=self.namespace
                )
                logger.debug("Existing entries deleted successfully")
            except Exception as e:
                if "Namespace not found" not in str(e):
                    logger.warning(f"Could not delete existing entries: {e}")
            
            # Create SINGLE vector entry for this resource
            logger.debug("Preparing single record for Pinecone upsert...")
            
            # Create the vector record for text-based embedding
            vector_record = {
                '_id': f"{resource_id}",  # Use _id for upsert_records
                conf.VECTOR_TEXT_FIELD: doc_content,  # The text field that will be embedded
                **metadata  # Spread the metadata as fields
            }
            
            logger.debug("Starting Pinecone upsert for single resource...")
            try:
                # Use Pinecone's upsert_records for automatic text embedding
                upsert_response = self.index.upsert_records(
                    namespace=self.namespace,
                    records=[vector_record]
                )
                logger.info(f"Successfully embedded resource {resource_id} as single vector")
                logger.debug(f"Upsert response: {upsert_response}")
                
            except Exception as e:
                logger.error(f"Pinecone upsert failed: {e}")
                return False
            
            logger.info(f"✅ Enhanced vector embedding completed for resource {resource_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error in enhanced vector embedding for resource {resource_id}: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def _create_resource_profile(self, 
                               resource_id: str,
                               resource_metadata: Dict[str, Any],
                               dataset_metadata: Dict[str, Any],
                               stats_data: Dict[str, Any],
                               freq_data: Dict[str, Any],
                               temporal_info: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Create resource profile from datapusher analysis"""
        
        profile = {
            'resource_id': resource_id,
            'resource_name': resource_metadata.get('name', resource_id),
            'format': resource_metadata.get('format', 'unknown'),
            'record_count': resource_metadata.get('record_count', 0),
            'dataset_id': dataset_metadata.get('id'),
            'dataset_title': dataset_metadata.get('title'),
            'dataset_tags': [tag['name'] if isinstance(tag, dict) else str(tag) 
                           for tag in dataset_metadata.get('tags', [])],
            'dataset_notes': dataset_metadata.get('notes', ''),
            'resource_description': resource_metadata.get('description', ''),
            'stats_summary': stats_data,
            'frequency_summary': self._summarize_frequencies(freq_data),
            'profiling_timestamp': datetime.now().isoformat(),
            'temporal_coverage': temporal_info
        }
        
        # Extract column information from stats
        if stats_data:
            profile['columns_info'] = self._extract_column_info(stats_data)
            profile['data_patterns'] = self._detect_patterns_from_stats(stats_data)
        
        return profile

    def _extract_column_info(self, stats_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract column information from qsv stats"""
        columns_info = {}
        
        for field_name, field_stats in stats_data.items():
            if isinstance(field_stats, dict) and field_stats.get('stats'):
                stats = field_stats['stats']
                
                col_info = {
                    'dtype': stats.get('type', 'String'),
                    'non_null_count': self._safe_int(stats.get('count', 0)),
                    'null_count': self._safe_int(stats.get('nullcount', 0)),
                    'unique_count': self._safe_int(stats.get('cardinality', 0)),
                    'is_numeric': stats.get('type') in ['Integer', 'Float'],
                }
                
                # Add numeric stats if available
                if col_info['is_numeric']:
                    col_info['numeric_stats'] = {
                        'min': self._safe_float(stats.get('min', 0)),
                        'max': self._safe_float(stats.get('max', 0)),
                        'mean': self._safe_float(stats.get('mean')) if stats.get('mean') else None,
                    }
                
                # Check if geographic
                col_info['is_geographic'] = self._is_geographic_column(field_name)
                
                columns_info[field_name] = col_info
        
        return columns_info

    def _is_geographic_column(self, col_name: str) -> bool:
        """Check if column name suggests geographic data"""
        col_name_lower = str(col_name).lower()
        geo_keywords = ['county', 'state', 'city', 'country', 'region', 'area', 
                       'district', 'location', 'address', 'zip', 'postal', 'fips',
                       'place', 'metro', 'msa', 'municipality', 'province', 'territory',
                       'geoid', 'geography', 'geographic', 'spatial']
        
        return any(keyword in col_name_lower for keyword in geo_keywords)

    def _detect_patterns_from_stats(self, stats_data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect data patterns from stats"""
        patterns = {
            'has_geographic': False,
            'has_financial': False,
            'has_demographic': False,
            'has_temporal': False,
            'data_categories': []
        }
        
        financial_keywords = ['income', 'revenue', 'cost', 'price', 'amount', 'salary', 
                            'wage', 'payment', 'balance', 'budget', 'earning', 'median',
                            'expenditure', 'expense', 'profit', 'tax', 'gdp', 'economic']
        
        demographic_keywords = ['population', 'age', 'gender', 'race', 'ethnicity', 
                              'education', 'household', 'family', 'marriage', 'birth', 'death',
                              'male', 'female', 'demographic', 'people', 'person']
        
        temporal_keywords = ['year', 'date', 'time', 'month', 'day', 'quarter', 'period']
        
        for field_name, field_info in stats_data.items():
            if isinstance(field_info, dict):
                field_lower = field_name.lower()
                
                # Check geographic (including state names and geographic identifiers)
                if self._is_geographic_column(field_name):
                    patterns['has_geographic'] = True
                    if 'geographic' not in patterns['data_categories']:
                        patterns['data_categories'].append('geographic')
                
                # Additional check for geographic data - look at common geographic values
                elif field_name.lower() == 'name' and field_info.get('stats', {}).get('cardinality'):
                    # If 'name' field with reasonable cardinality, check if it might contain place names
                    cardinality = self._safe_int(field_info.get('stats', {}).get('cardinality', 0))
                    if 10 <= cardinality <= 200:  # Reasonable range for states, countries, etc.
                        patterns['has_geographic'] = True
                        if 'geographic' not in patterns['data_categories']:
                            patterns['data_categories'].append('geographic')
                
                # Check temporal
                if any(keyword in field_lower for keyword in temporal_keywords):
                    patterns['has_temporal'] = True
                    if 'temporal' not in patterns['data_categories']:
                        patterns['data_categories'].append('temporal')
                
                # Check financial
                if any(keyword in field_lower for keyword in financial_keywords):
                    patterns['has_financial'] = True
                    if 'financial' not in patterns['data_categories']:
                        patterns['data_categories'].append('financial')
                
                # Check demographic
                if any(keyword in field_lower for keyword in demographic_keywords):
                    patterns['has_demographic'] = True
                    if 'demographic' not in patterns['data_categories']:
                        patterns['data_categories'].append('demographic')
        
        return patterns

    def _generate_ai_description_with_full_data(self, profile: Dict[str, Any], 
                                              stats_data: Dict[str, Any], 
                                              freq_data: Dict[str, Any], 
                                              sample_data: str = None) -> tuple:
        """Generate comprehensive AI description and tags using full QSV analysis"""
        
        # Build comprehensive context for LLM
        context_parts = [
            f"Dataset: {profile['dataset_title']}",
            f"Resource: {profile['resource_name']} ({profile['format']})",
            f"Records: {profile.get('record_count', 'Unknown')}",
            ""
        ]
        
        # Add original descriptions if available
        if profile.get('dataset_notes'):
            context_parts.extend([
                "Dataset Description:",
                profile['dataset_notes'][:800],  # Limit length
                ""
            ])
        
        # Add comprehensive QSV statistics
        if stats_data:
            context_parts.append("COLUMN STATISTICS:")
            for field_name, field_stats in list(stats_data.items())[:20]:  # Limit columns
                if isinstance(field_stats, dict) and 'stats' in field_stats:
                    stats = field_stats['stats']
                    context_parts.append(f"- {field_name}: {stats.get('type', 'unknown')} | "
                                       f"NULL: {stats.get('nullcount', 0)} | "
                                       f"Unique: {stats.get('cardinality', 'unknown')}")
                    
                    # Add range for numeric columns
                    if stats.get('type') in ['Integer', 'Float']:
                        min_val = stats.get('min', '')
                        max_val = stats.get('max', '')
                        if min_val and max_val:
                            context_parts.append(f"  Range: {min_val} to {max_val}")
                            
            context_parts.append("")
        
        # Add frequency analysis (top values)
        if freq_data:
            context_parts.append("TOP VALUES BY COLUMN:")
            for field_name, frequencies in list(freq_data.items())[:15]:  # Limit columns
                if isinstance(frequencies, list) and frequencies:
                    top_values = [str(freq.get('value', '')) for freq in frequencies[:5]]
                    context_parts.append(f"- {field_name}: {', '.join(top_values)}")
            context_parts.append("")
        
        # Add sample data if available
        if sample_data and conf.VECTOR_INCLUDE_SAMPLE_DATA:
            context_parts.extend([
                "SAMPLE DATA:",
                sample_data[:1000],  # Limit sample size
                ""
            ])
        
        # Build the context string
        full_context = "\n".join(context_parts)
        
        # Limit total context to prevent API issues
        if len(full_context) > conf.VECTOR_MAX_CONTEXT_LENGTH:
            full_context = full_context[:conf.VECTOR_MAX_CONTEXT_LENGTH] + "..."
        
        # Generate description
        description_prompt = f"""Analyze this dataset and provide:

{full_context}

Based on the statistics, column names, data types, and sample values above, generate:

1. A clear, concise description (4-5 sentences) explaining:
   - What specific data this contains
   - Key variables and their meaning
   - The scope/coverage of the data

2. Generate {conf.VECTOR_MAX_AI_TAGS} relevant search keywords/tags that would help users find this dataset.

Format as JSON:
{{
  "description": "Your description here",
  "tags": ["tag1", "tag2", "tag3", ...]
}}"""

        log.debug(f"Generated prompt length: {len(description_prompt)} characters")
        log.debug(f"Context length: {len(full_context)} characters")

        try:
            response = self._call_openrouter(description_prompt)
            
            if response and response.strip():
                try:
                    # Clean response - remove markdown code fences if present
                    cleaned_response = response.strip()
                    if cleaned_response.startswith('```json'):
                        # Remove opening ```json
                        cleaned_response = cleaned_response[7:]
                    if cleaned_response.startswith('```'):
                        # Remove opening ``` (without json)
                        cleaned_response = cleaned_response[3:]
                    if cleaned_response.endswith('```'):
                        # Remove closing ```
                        cleaned_response = cleaned_response[:-3]
                    
                    cleaned_response = cleaned_response.strip()
                    log.debug(f"Cleaned response length: {len(cleaned_response)} characters")
                    
                    # Parse JSON response
                    result = json.loads(cleaned_response)
                    description = result.get('description', '')
                    tags = result.get('tags', [])
                    
                    # Validate description length
                    if len(description) < conf.VECTOR_MIN_DESCRIPTION_LENGTH:
                        log.warning(f"AI description too short ({len(description)} chars), using fallback")
                        description = self._generate_fallback_description(profile)
                    
                    log.info(f"AI description generated successfully: {len(description)} chars, {len(tags)} tags")
                    return description, tags[:conf.VECTOR_MAX_AI_TAGS]
                    
                except json.JSONDecodeError as e:
                    log.warning(f"Failed to parse AI response as JSON: {e}")
                    log.debug(f"Cleaned response: {cleaned_response[:200]}...")
            else:
                log.warning("OpenRouter returned empty response")
            
        except Exception as e:
            log.warning(f"Failed to generate AI description: {e}")
        
        # Fallback description and tags
        log.info("Using fallback description and tags due to AI generation failure")
        return self._generate_fallback_description(profile), self._generate_fallback_tags(profile, stats_data)

    def _generate_fallback_description(self, profile: Dict[str, Any]) -> str:
        """Generate fallback description when AI fails"""
        record_count = profile.get('record_count', 'unknown')
        resource_name = profile.get('resource_name', 'resource')
        dataset_title = profile.get('dataset_title', 'dataset')
        format_type = profile.get('format', 'unknown')
        
        description = f"This {format_type} resource named '{resource_name}' contains {record_count} records with data related to {dataset_title}."
        
        # Add pattern information if available
        patterns = profile.get('data_patterns', {})
        if patterns.get('data_categories'):
            categories = ', '.join(patterns['data_categories'])
            description += f" The data includes {categories} information."
            
        log.debug(f"Generated fallback description: {len(description)} characters")
        return description

    def _generate_fallback_tags(self, profile: Dict[str, Any], stats_data: Dict[str, Any]) -> List[str]:
        """Generate fallback tags when AI fails"""
        fallback_tags = []
        
        # Extract basic tags from title and column names
        dataset_title = profile.get('dataset_title', '')
        if dataset_title:
            title_words = dataset_title.lower().split()
            fallback_tags.extend([word for word in title_words if len(word) > 3 and word.isalpha()])
        
        # Add resource name words
        resource_name = profile.get('resource_name', '')
        if resource_name:
            name_words = resource_name.lower().split()
            fallback_tags.extend([word for word in name_words if len(word) > 3 and word.isalpha()])
        
        # Add pattern-based tags
        patterns = profile.get('data_patterns', {})
        if patterns.get('has_demographic'):
            fallback_tags.append('demographic')
        if patterns.get('has_geographic'):
            fallback_tags.append('geographic')
        if patterns.get('has_financial'):
            fallback_tags.append('financial')
        if patterns.get('has_temporal'):
            fallback_tags.append('temporal')
        
        if stats_data:
            # Add column names as potential tags (first few only)
            col_tags = [col.lower() for col in list(stats_data.keys())[:5] 
                       if len(col) > 2 and col.isalpha()]
            fallback_tags.extend(col_tags)
        
        # Remove duplicates and limit
        unique_tags = list(set(fallback_tags))[:conf.VECTOR_MAX_AI_TAGS]
        log.debug(f"Generated {len(unique_tags)} fallback tags: {', '.join(unique_tags[:5])}...")
        return unique_tags

    def _analyze_columns_from_qsv(self, stats_data: Dict[str, Any], freq_data: Dict[str, Any]) -> str:
        """Create detailed column analysis from QSV data optimized for AI agent understanding"""
        if not stats_data:
            return ""
        
        analysis_parts = []
        
        for field_name, field_stats in list(stats_data.items())[:20]:  # Increase limit for better schema understanding
            if isinstance(field_stats, dict) and 'stats' in field_stats:
                stats = field_stats['stats']
                col_type = stats.get('type', 'unknown')
                
                # Basic column info
                col_info = f"{field_name}({col_type})"
                
                # Add cardinality/uniqueness info - important for AI agents
                cardinality = self._safe_int(stats.get('cardinality', 0))
                if cardinality:
                    col_info += f":{cardinality}vals"
                
                # Add null info if significant
                null_count = self._safe_int(stats.get('nullcount', 0))
                if null_count > 0:
                    col_info += f",{null_count}nulls"
                
                # Add range for numeric columns
                if col_type in ['Integer', 'Float']:
                    min_val = self._safe_float(stats.get('min'))
                    max_val = self._safe_float(stats.get('max'))
                    if min_val != 0.0 or max_val != 0.0:  # Check if we have actual values
                        col_info += f"[{min_val}-{max_val}]"
                
                # Add sample values for categorical columns with low cardinality
                elif field_name in freq_data and freq_data[field_name] and cardinality <= 20:
                    top_vals = [str(f.get('value', ''))[:15] for f in freq_data[field_name][:3]]  # Truncate long values
                    if top_vals:
                        col_info += f"({','.join(top_vals)})"
                
                analysis_parts.append(col_info)
        
        return "; ".join(analysis_parts)

    def _extract_frequency_insights(self, freq_data: Dict[str, Any]) -> str:
        """Extract key insights from frequency data focused on data understanding"""
        if not freq_data:
            return ""
        
        insights = []
        
        for field_name, frequencies in list(freq_data.items())[:8]:  # Limit fields
            if isinstance(frequencies, list) and frequencies:
                # Get unique count and top values
                unique_count = len(frequencies)
                top_freq = frequencies[0] if frequencies else {}
                top_value = str(top_freq.get('value', ''))
                
                # Focus on meaningful patterns for AI agents
                if unique_count == 1:
                    insights.append(f"{field_name}: constant value '{top_value}'")
                elif unique_count <= 10:
                    # Small set of categories - list them
                    values = [str(f.get('value', '')) for f in frequencies[:5]]
                    insights.append(f"{field_name}: categorical with {unique_count} values ({', '.join(values)})")
                elif unique_count <= 100:
                    insights.append(f"{field_name}: {unique_count} distinct values, most common: '{top_value}'")
                else:
                    # High cardinality - likely identifier or continuous
                    insights.append(f"{field_name}: high cardinality ({unique_count} values) - likely unique identifier or continuous data")
        
        return "; ".join(insights)

    def _create_enhanced_document_content(self, profile: Dict[str, Any]) -> str:
        """Create comprehensive single-chunk content optimized for agent search"""
        
        # Start with core identification - what agents need first
        content_parts = [
            f"Resource: {profile['resource_name']}",
            f"Dataset: {profile['dataset_title']}", 
            f"Format: {profile['format']} | Records: {profile.get('record_count', 'Unknown')}",
        ]
        
        # Add AI-generated description (from full QSV analysis)
        if profile.get('ai_description'):
            content_parts.extend([
                "Description: " + profile['ai_description']
            ])
        
        # Add AI-generated tags and keywords for searchability
        if profile.get('ai_tags'):
            content_parts.extend([
                f"Keywords: {', '.join(profile['ai_tags'])}"
            ])
        
        # Add detailed column analysis from QSV stats
        if profile.get('column_analysis'):
            content_parts.extend([
                "Schema: " + profile['column_analysis']
            ])
        
        # Add frequency insights (top values from each column)
        if profile.get('frequency_insights'):
            content_parts.extend([
                "Data Structure: " + profile['frequency_insights']
            ])
        
        # Add temporal coverage if available
        if profile.get('temporal_coverage'):
            temp_info = profile['temporal_coverage']
            content_parts.extend([
                f"Time Coverage: {temp_info.get('min', 'Unknown')} to {temp_info.get('max', 'Unknown')} ({temp_info.get('year_count', 'Unknown')} years)"
            ])
        
        # Add data characteristics flags for filtering
        characteristics = []
        patterns = profile.get('data_patterns', {})
        if patterns.get('has_geographic'):
            characteristics.append("Geographic")
        if patterns.get('has_temporal'):
            characteristics.append("Time-series")
        if patterns.get('has_financial'):
            characteristics.append("Financial")
        if patterns.get('has_demographic'):
            characteristics.append("Demographic")
        
        if characteristics:
            content_parts.append(f"Data Types: {', '.join(characteristics)}")
        
        # Add original dataset tags for context
        if profile.get('dataset_tags'):
            content_parts.append(f"Tags: {', '.join(profile['dataset_tags'])}")
        
        # Join with spaces instead of newlines for cleaner text
        full_content = " | ".join(content_parts)
        
        # Target optimal length for embedding quality with 2048 dimensions
        if len(full_content) > conf.VECTOR_CONTENT_TARGET_LENGTH:
            # Prioritize: AI description > schema > data structure
            full_content = full_content[:conf.VECTOR_CONTENT_TARGET_LENGTH] + "..."
        
        return full_content

    def _create_enhanced_metadata(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Create comprehensive metadata optimized for vector search filtering"""
        metadata = {
            # Core identifiers
            'resource_id': str(profile['resource_id']),
            'dataset_id': str(profile.get('dataset_id', '')),
            'resource_name': str(profile.get('resource_name', '')),
            'dataset_title': str(profile.get('dataset_title', '')),
            'format': str(profile.get('format', 'unknown')),
            
            # Content metrics for filtering
            'record_count': self._safe_int(profile.get('record_count', 0)),
            'column_count': len(profile.get('columns_info', {})),
            
            # Data pattern flags for precise filtering
            'has_temporal': bool(profile.get('data_patterns', {}).get('has_temporal', False)),
            'has_geographic': bool(profile.get('data_patterns', {}).get('has_geographic', False)),
            'has_financial': bool(profile.get('data_patterns', {}).get('has_financial', False)),
            'has_demographic': bool(profile.get('data_patterns', {}).get('has_demographic', False)),
            
            # Timestamps
            'indexed_at': datetime.now().isoformat(),
            'last_updated': profile.get('resource_metadata', {}).get('last_modified', datetime.now().isoformat()),
        }
        
        # Add temporal metadata for time-based filtering
        if profile.get('temporal_coverage'):
            temp_info = profile['temporal_coverage']
            if temp_info.get('min'):
                metadata['temporal_min'] = self._safe_int(temp_info['min'])
            if temp_info.get('max'):
                metadata['temporal_max'] = self._safe_int(temp_info['max'])
            if temp_info.get('year_count'):
                metadata['year_count'] = self._safe_int(temp_info['year_count'])
        
        # Add domain-specific metadata
        if profile.get('data_patterns', {}).get('data_categories'):
            # Convert categories to boolean flags for easier filtering
            categories = profile['data_patterns']['data_categories']
            metadata['has_economic_data'] = 'financial' in categories
            metadata['has_social_data'] = 'demographic' in categories
        
        # Store AI tags as simple string for Pinecone compatibility
        if profile.get('ai_tags'):
            metadata['ai_tags'] = ', '.join(profile['ai_tags'][:10])  # Limit and join for search
        
        return metadata

    def _summarize_frequencies(self, freq_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Summarize frequency data for embedding"""
        summary = {}
        
        for field_name, frequencies in freq_data.items():
            if frequencies and len(frequencies) > 0:
                # Get top 5 values with safe count conversion
                top_values = [f"{item['value']} ({self._safe_int(item.get('count', 0))})" 
                             for item in frequencies[:5]]
                summary[field_name] = {
                    'top_values': top_values,
                    'unique_count': len(frequencies)
                }
        
        return summary

    def delete_resource_embeddings(self, resource_id: str) -> bool:
        """Delete embeddings for a resource"""
        if not self.enabled:
            return False
            
        try:
            # Delete vectors by resource_id filter
            self.index.delete(
                filter={"resource_id": resource_id},
                namespace=self.namespace
            )
            log.info(f"Deleted embeddings for resource {resource_id}")
            return True
        except Exception as e:
            log.error(f"Error deleting embeddings for resource {resource_id}: {e}")
            return False
