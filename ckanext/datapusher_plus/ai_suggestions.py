# encoding: utf-8
# flake8: noqa: E501

"""
AI-powered suggestions generator for DataPusher Plus
Generates intelligent descriptions and tags based on QSV data analysis
"""

import json
import logging
import requests
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

import ckanext.datapusher_plus.config as conf

log = logging.getLogger(__name__)


class AIDescriptionGenerator:
    """Generate AI-powered descriptions and tags using full QSV analysis"""
    
    def __init__(self, logger=None):
        self.enabled = conf.ENABLE_AI_SUGGESTIONS
        self.logger = logger or log
        
        if not self.enabled:
            self.logger.info("AI suggestions are disabled")
            return
            
        # Validate API key
        if not conf.OPENROUTER_API_KEY:
            self.logger.warning("OpenRouter API key is not configured - AI suggestions will be disabled")
            self.enabled = False
            return
            
        self.openrouter_api_key = conf.OPENROUTER_API_KEY
        self.openrouter_model = conf.OPENROUTER_MODEL
        self.openrouter_base_url = conf.OPENROUTER_BASE_URL
        
        self.logger.info(f"AI suggestions initialized with model: {self.openrouter_model}")
    
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
            temperature = float(conf.AI_TEMPERATURE)
        if max_tokens is None:
            max_tokens = conf.AI_MAX_TOKENS
        
        headers = {
            "Authorization": f"Bearer {self.openrouter_api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/dathere/datapusher-plus",
            "X-Title": "DataPusher Plus AI Suggestions"
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
        
        self.logger.debug(f"OpenRouter request: model={self.openrouter_model}, temp={temperature}, max_tokens={max_tokens}")
        self.logger.debug(f"Prompt length: {len(prompt)} characters")
        
        try:
            self.logger.debug("Calling OpenRouter API...")
            response = requests.post(
                f"{self.openrouter_base_url}/chat/completions",
                headers=headers,
                json=data,
                timeout=conf.AI_TIMEOUT
            )
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    content = result.get('choices', [{}])[0].get('message', {}).get('content', '')
                    if content:
                        self.logger.debug(f"OpenRouter response received: {len(content)} characters")
                        return content.strip()
                    else:
                        self.logger.warning("OpenRouter returned empty content in response")
                        self.logger.debug(f"Full response structure: {result}")
                        return ""
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to parse OpenRouter JSON response: {e}")
                    self.logger.debug(f"Response text: {response.text[:500]}")
                    return ""
            else:
                self.logger.error(f"OpenRouter API error {response.status_code}: {response.text[:500]}")
                return ""
                
        except requests.exceptions.Timeout:
            self.logger.error(f"OpenRouter API timeout after {conf.AI_TIMEOUT} seconds")
            return ""
        except requests.exceptions.RequestException as e:
            self.logger.error(f"OpenRouter API request error: {e}")
            return ""
        except Exception as e:
            self.logger.error(f"Error calling OpenRouter: {e}")
            return ""

    def generate_ai_suggestions(
        self,
        resource_metadata: Dict[str, Any],
        dataset_metadata: Dict[str, Any],
        stats_data: Dict[str, Any],
        freq_data: Dict[str, Any],
        dataset_stats: Dict[str, Any],
        sample_data: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate comprehensive AI suggestions for dataset fields in a single LLM call
        
        Returns a dictionary with field names as keys and suggestion data as values
        """
        if not self.enabled:
            return {}
            
        try:
            self.logger.info("Starting AI suggestion generation (single LLM call)...")
            start_time = datetime.now()
            
            # Build comprehensive context for LLM
            context = self._build_context(
                resource_metadata, dataset_metadata, stats_data, 
                freq_data, dataset_stats, sample_data
            )
            
            # Check if we need title and notes
            current_title = dataset_metadata.get('title', '')
            skip_title = current_title and len(current_title) > 10
            
            # Check for geographic data
            has_geo_data = self._has_geographic_data(stats_data)
            
            # Generate ALL suggestions in a single LLM call
            suggestions = self._generate_all_suggestions_single_call(
                context, 
                dataset_metadata, 
                skip_title=skip_title,
                include_spatial=has_geo_data
            )
            
            elapsed = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"AI suggestions generated: {len(suggestions)} fields in {elapsed:.2f}s")
            return suggestions
            
        except Exception as e:
            self.logger.error(f"Error generating AI suggestions: {e}")
            if conf.AI_FALLBACK_ON_FAILURE:
                return self._generate_fallback_suggestions(
                    resource_metadata, dataset_metadata, stats_data
                )
            return {}
    
    def _generate_all_suggestions_single_call(
        self,
        context: str,
        dataset_metadata: Dict[str, Any],
        skip_title: bool = False,
        include_spatial: bool = False
    ) -> Dict[str, Any]:
        """
        Generate all suggestions in a single LLM call using structured JSON output
        """
        
        # Build the prompt requesting JSON output with all fields
        fields_to_generate = []
        if not skip_title:
            fields_to_generate.append('"title": "A clear, concise dataset title (max 100 chars)"')
        fields_to_generate.append('"notes": "4-6 sentence comprehensive description explaining what data this contains, key variables, scope/coverage, and notable patterns"')
        fields_to_generate.append(f'"tags": ["tag1", "tag2", ...] // Array of {conf.AI_MAX_TAGS} relevant keywords for search"')
        if include_spatial:
            fields_to_generate.append('"spatial_extent": "2-3 sentence description of geographic coverage"')
        fields_to_generate.append('"additional_information": "2-4 sentences about data quality, patterns, use cases, and caveats"')
        
        fields_json_template = ",\n  ".join(fields_to_generate)
        
        system_prompt = """You are a data cataloging expert. Analyze datasets and generate comprehensive metadata to help users understand and discover the data. Be specific, factual, and professional."""
        
        prompt = f"""Analyze this dataset and generate metadata suggestions in JSON format:

{context}

Generate a JSON object with the following fields:
{{
  {fields_json_template}
}}

Important:
- Base suggestions on the actual column names, data types, statistics, and sample values provided
- Be specific about what data is present (e.g., mention specific variables, date ranges if visible, geographic areas if identifiable)
- For tags, focus on subject matter, data types, and potential use cases
- Write professionally and informatively
- Return ONLY valid JSON, nothing else"""

        try:
            self.logger.debug("Calling LLM for all suggestions in single call...")
            response = self._call_openrouter(
                prompt, 
                system_prompt=system_prompt,
                temperature=0.7,
                max_tokens=1500  # Increased for comprehensive response
            )
            
            if not response:
                self.logger.warning("Empty response from LLM")
                return {}
            
            # Clean response - remove markdown code fences if present
            cleaned_response = response.strip()
            if cleaned_response.startswith('```json'):
                cleaned_response = cleaned_response[7:]
            if cleaned_response.startswith('```'):
                cleaned_response = cleaned_response[3:]
            if cleaned_response.endswith('```'):
                cleaned_response = cleaned_response[:-3]
            cleaned_response = cleaned_response.strip()
            
            # Parse JSON response
            try:
                result = json.loads(cleaned_response)
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse LLM JSON response: {e}")
                self.logger.debug(f"Response was: {cleaned_response[:500]}")
                return {}
            
            # Convert to our suggestion format
            suggestions = {}
            
            if 'title' in result and result['title']:
                title = str(result['title']).strip().strip('"\'')
                if len(title) > 5:
                    suggestions['title'] = {
                        'value': title[:100],  # Enforce max length
                        'source': 'AI Generated',
                        'confidence': 'high'
                    }
                    self.logger.debug(f"Title: {len(title)} chars")
            
            if 'notes' in result and result['notes']:
                notes = str(result['notes']).strip()
                if len(notes) >= conf.AI_MIN_DESCRIPTION_LENGTH:
                    suggestions['notes'] = {
                        'value': notes,
                        'source': 'AI Generated',
                        'confidence': 'high'
                    }
                    self.logger.debug(f"Notes: {len(notes)} chars")
            
            if 'tags' in result and result['tags']:
                if isinstance(result['tags'], list):
                    tags = [str(tag).lower().strip() for tag in result['tags'][:conf.AI_MAX_TAGS]]
                    if tags:
                        suggestions['primary_tags'] = {
                            'value': ', '.join(tags),
                            'source': 'AI Generated',
                            'confidence': 'medium'
                        }
                        self.logger.debug(f"Tags: {len(tags)} tags")
                elif isinstance(result['tags'], str):
                    # Handle case where LLM returned comma-separated string
                    tags = [tag.strip().lower() for tag in result['tags'].split(',')[:conf.AI_MAX_TAGS]]
                    if tags:
                        suggestions['primary_tags'] = {
                            'value': ', '.join(tags),
                            'source': 'AI Generated',
                            'confidence': 'medium'
                        }
                        self.logger.debug(f"Tags: {len(tags)} tags (from string)")
            
            if include_spatial and 'spatial_extent' in result and result['spatial_extent']:
                spatial = str(result['spatial_extent']).strip()
                if len(spatial) > 20:
                    suggestions['spatial_extent'] = {
                        'value': spatial,
                        'source': 'AI Generated',
                        'confidence': 'medium'
                    }
                    self.logger.debug(f"Spatial extent: {len(spatial)} chars")
            
            if 'additional_information' in result and result['additional_information']:
                additional = str(result['additional_information']).strip()
                if len(additional) > 30:
                    suggestions['additional_information'] = {
                        'value': additional,
                        'source': 'AI Generated',
                        'confidence': 'medium'
                    }
                    self.logger.debug(f"Additional info: {len(additional)} chars")
            
            self.logger.info(f"Successfully parsed {len(suggestions)} suggestions from single LLM call")
            return suggestions
            
        except Exception as e:
            self.logger.error(f"Error in single-call suggestion generation: {e}")
            return {}

    def _build_context(
        self,
        resource_metadata: Dict[str, Any],
        dataset_metadata: Dict[str, Any],
        stats_data: Dict[str, Any],
        freq_data: Dict[str, Any],
        dataset_stats: Dict[str, Any],
        sample_data: Optional[str]
    ) -> str:
        """Build comprehensive context for LLM"""
        context_parts = []
        
        # Basic information
        context_parts.extend([
            f"Resource: {resource_metadata.get('name', 'Unknown')} ({resource_metadata.get('format', 'unknown')})",
            f"Dataset: {dataset_metadata.get('title', 'Unknown')}",
            f"Records: {dataset_stats.get('RECORD_COUNT', 'Unknown')}",
            ""
        ])
        
        # Add existing descriptions if available
        if dataset_metadata.get('notes'):
            context_parts.extend([
                "Existing Dataset Description:",
                dataset_metadata['notes'][:800],  # Limit length
                ""
            ])
        
        # Add comprehensive QSV statistics
        if stats_data:
            context_parts.append("COLUMN STATISTICS:")
            for field_name, field_stats in list(stats_data.items())[:20]:  # Limit columns
                if isinstance(field_stats, dict) and 'stats' in field_stats:
                    stats = field_stats['stats']
                    context_parts.append(
                        f"- {field_name}: {stats.get('type', 'unknown')} | "
                        f"NULL: {stats.get('nullcount', 0)} | "
                        f"Unique: {stats.get('cardinality', 'unknown')}"
                    )
                    
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
        if sample_data and conf.AI_INCLUDE_SAMPLE_DATA:
            context_parts.extend([
                "SAMPLE DATA:",
                sample_data[:1000],  # Limit sample size
                ""
            ])
        
        # Build the context string
        full_context = "\n".join(context_parts)
        
        # Limit total context to prevent API issues
        if len(full_context) > conf.AI_MAX_CONTEXT_LENGTH:
            full_context = full_context[:conf.AI_MAX_CONTEXT_LENGTH] + "..."
        
        return full_context

    def _has_geographic_data(self, stats_data: Dict[str, Any]) -> bool:
        """Check if dataset contains geographic data"""
        geo_keywords = ['county', 'state', 'city', 'country', 'region', 'latitude', 'longitude', 
                       'lat', 'lon', 'location', 'address', 'zip', 'postal']
        
        for field_name in stats_data.keys():
            field_lower = field_name.lower()
            if any(keyword in field_lower for keyword in geo_keywords):
                return True
        return False

    def _generate_fallback_suggestions(
        self,
        resource_metadata: Dict[str, Any],
        dataset_metadata: Dict[str, Any],
        stats_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate simple fallback suggestions when AI fails"""
        suggestions = {}
        
        # Simple title based on resource name
        resource_name = resource_metadata.get('name', 'Resource')
        dataset_title = dataset_metadata.get('title', '')
        
        if not dataset_title or len(dataset_title) < 10:
            suggestions['title'] = {
                'value': f"Data from {resource_name}",
                'source': 'Auto-generated',
                'confidence': 'low'
            }
        
        # Simple description
        record_count = resource_metadata.get('record_count', 'unknown')
        format_type = resource_metadata.get('format', 'unknown')
        
        suggestions['notes'] = {
            'value': f"This {format_type} dataset contains {record_count} records with data related to {resource_name}.",
            'source': 'Auto-generated',
            'confidence': 'low'
        }
        
        # Simple tags from column names
        if stats_data:
            col_tags = [col.lower() for col in list(stats_data.keys())[:5] 
                       if len(col) > 2 and col.replace('_', '').isalpha()]
            if col_tags:
                suggestions['primary_tags'] = {
                    'value': ', '.join(col_tags),
                    'source': 'Auto-generated',
                    'confidence': 'low'
                }
        
        self.logger.info(f"Generated {len(suggestions)} fallback suggestions")
        return suggestions
