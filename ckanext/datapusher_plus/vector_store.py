# -*- coding: utf-8 -*-
"""
Vector Store integration for DataPusher Plus
Embeds resource data and metadata during the upload process
Using Pinecone vector database and OpenRouter for LLM
"""

import os
import logging
import json
import requests
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from pinecone import Pinecone
import ckanext.datapusher_plus.config as conf

log = logging.getLogger(__name__)


class DataPusherVectorStore:
    """Vector store for embedding resources during datapusher upload"""
    
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
            self.openrouter_base_url = "https://openrouter.ai/api/v1"
            
            # Text splitter settings
            self.chunk_size = conf.CHUNK_SIZE
            self.chunk_overlap = conf.CHUNK_OVERLAP
            
            log.info("Vector store components initialized successfully")
            
        except Exception as e:
            log.error(f"Failed to initialize vector store: {e}")
            self.enabled = False
    
    def _split_text(self, text: str, chunk_size: int = None, chunk_overlap: int = None) -> List[str]:
        """Simple text splitter"""
        if chunk_size is None:
            chunk_size = self.chunk_size
        if chunk_overlap is None:
            chunk_overlap = self.chunk_overlap
            
        if len(text) <= chunk_size:
            return [text]
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + chunk_size
            
            # Try to find a good break point
            if end < len(text):
                # Look for paragraph break
                break_point = text.rfind('\n\n', start, end)
                if break_point == -1:
                    # Look for sentence break
                    break_point = text.rfind('. ', start, end)
                if break_point == -1:
                    # Look for any newline
                    break_point = text.rfind('\n', start, end)
                if break_point > start:
                    end = break_point + 1
            
            chunks.append(text[start:end].strip())
            start = end - chunk_overlap
        
        return chunks
    
    def _call_openrouter(self, prompt: str, system_prompt: str = None, 
                        temperature: float = 0.1, max_tokens: int = 500) -> str:
        """Call OpenRouter API for text generation"""
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
        
        try:
            response = requests.post(
                f"{self.openrouter_base_url}/chat/completions",
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                return result["choices"][0]["message"]["content"].strip()
            else:
                log.error(f"OpenRouter API error {response.status_code}: {response.text}")
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
                      logger=None) -> bool:
        """Embed a resource after successful datastore upload"""
        
        if not self.enabled:
            return False
            
        if logger is None:
            logger = log
            
        try:
            logger.info(f"Starting vector embedding for resource {resource_id}")
            
            # Create resource profile from available data
            profile = self._create_resource_profile(
                resource_id, resource_metadata, dataset_metadata, 
                stats_data, freq_data, temporal_info
            )
            
            # Generate AI description using OpenRouter
            ai_description = self._generate_ai_description(profile)
            profile['ai_description'] = ai_description
            
            # Create document content
            doc_content = self._create_document_content(profile)
            
            # Create metadata for Pinecone
            metadata = self._create_metadata(profile)
            
            # Split document if too long
            chunks = self._split_text(doc_content)
            
            # Remove existing entries for this resource
            try:
                # Delete existing vectors for this resource
                existing_ids = [f"{resource_id}_{i}" for i in range(100)]  # Assume max 100 chunks
                self.index.delete(ids=existing_ids, namespace=self.namespace)
            except Exception as e:
                logger.warning(f"Could not delete existing entries: {e}")
            
            # Prepare data for Pinecone upsert
            records = []
            for i, chunk in enumerate(chunks):
                chunk_metadata = metadata.copy()
                chunk_metadata['chunk_index'] = i
                chunk_metadata['total_chunks'] = len(chunks)
                
                records.append({
                    "id": f"{resource_id}_{i}",
                    "text": chunk,
                    "metadata": chunk_metadata
                })
            
            # Upsert to Pinecone (embeddings are generated automatically)
            self.index.upsert_records(
                namespace=self.namespace,
                records=records
            )
            
            logger.info(f"Successfully embedded {len(chunks)} chunks for resource {resource_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error embedding resource {resource_id}: {e}")
            return False
    
    def search_resources(self, query: str, n_results: int = 10, 
                        filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Search for resources using Pinecone hosted embeddings"""
        try:
            # Build search query for Pinecone
            search_query = {
                "inputs": {"text": query},
                "top_k": n_results
            }
            
            # Add metadata filters if provided
            if filters:
                search_query["filter"] = filters
            
            # Search using Pinecone
            results = self.index.search(
                namespace=self.namespace,
                query=search_query
            )
            
            # Format results
            formatted_results = []
            if results and 'matches' in results:
                for match in results['matches']:
                    formatted_results.append({
                        'id': match.get('id'),
                        'content': match.get('text', ''),
                        'metadata': match.get('metadata', {}),
                        'score': match.get('score', 0.0),
                        'distance': 1.0 - match.get('score', 0.0)
                    })
            
            return formatted_results
            
        except Exception as e:
            log.error(f"Error searching resources: {e}")
            return []
    
    def search_resources_by_year(self, query: str, year_filter: tuple, 
                               n_results: int = 20) -> List[Dict[str, Any]]:
        """Search for resources containing specific years"""
        try:
            # Build filter for temporal filtering using Pinecone metadata filters
            filters = {
                "has_temporal": True,
                "temporal_min": {"$lte": year_filter[1]},
                "temporal_max": {"$gte": year_filter[0]}
            }
            
            # Search with year filter
            return self.search_resources(
                query=f"{query} {year_filter[0]} {year_filter[1]} year temporal data",
                n_results=n_results,
                filters=filters
            )
            
        except Exception as e:
            log.error(f"Error in year-based search: {e}")
            return []
    
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
            'dataset_id': dataset_metadata.get('id'),
            'dataset_title': dataset_metadata.get('title'),
            'dataset_tags': [tag['name'] if isinstance(tag, dict) else str(tag) 
                           for tag in dataset_metadata.get('tags', [])],
            'dataset_notes': dataset_metadata.get('notes', ''),
            'resource_description': resource_metadata.get('description', ''),
            'temporal_coverage': temporal_info,
            'stats_summary': stats_data,
            'frequency_summary': self._summarize_frequencies(freq_data),
            'profiling_timestamp': datetime.now().isoformat()
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
                    'non_null_count': int(stats.get('count', 0)),
                    'null_count': int(stats.get('nullcount', 0)),
                    'unique_count': int(stats.get('cardinality', 0)),
                    'is_temporal': stats.get('type') == 'DateTime',
                    'is_numeric': stats.get('type') in ['Integer', 'Float'],
                }
                
                # Add numeric stats if available
                if col_info['is_numeric']:
                    col_info['numeric_stats'] = {
                        'min': float(stats.get('min', 0)),
                        'max': float(stats.get('max', 0)),
                        'mean': float(stats.get('mean', 0)) if stats.get('mean') else None,
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
                       'place', 'metro', 'msa', 'municipality', 'province', 'territory']
        
        return any(keyword in col_name_lower for keyword in geo_keywords)
    
    def _detect_patterns_from_stats(self, stats_data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect data patterns from stats"""
        patterns = {
            'has_temporal': False,
            'has_geographic': False,
            'has_financial': False,
            'data_categories': []
        }
        
        financial_keywords = ['income', 'revenue', 'cost', 'price', 'amount', 'salary', 
                            'wage', 'payment', 'balance', 'budget', 'earning', 'median',
                            'expenditure', 'expense', 'profit', 'tax', 'gdp']
        
        for field_name, field_info in stats_data.items():
            if isinstance(field_info, dict):
                field_lower = field_name.lower()
                
                # Check temporal
                if field_info.get('stats', {}).get('type') == 'DateTime':
                    patterns['has_temporal'] = True
                
                # Check geographic
                if self._is_geographic_column(field_name):
                    patterns['has_geographic'] = True
                
                # Check financial
                if any(keyword in field_lower for keyword in financial_keywords):
                    patterns['has_financial'] = True
                    if 'financial' not in patterns['data_categories']:
                        patterns['data_categories'].append('financial')
                
                # Check unemployment
                if 'unemploy' in field_lower:
                    if 'unemployment' not in patterns['data_categories']:
                        patterns['data_categories'].append('unemployment')
        
        return patterns
    
    def _summarize_frequencies(self, freq_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Summarize frequency data for embedding"""
        summary = {}
        
        for field_name, frequencies in freq_data.items():
            if frequencies and len(frequencies) > 0:
                # Get top 5 values
                top_values = [f"{item['value']} ({item['count']})" 
                             for item in frequencies[:5]]
                summary[field_name] = {
                    'top_values': top_values,
                    'unique_count': len(frequencies)
                }
        
        return summary
    
    def _generate_ai_description(self, profile: Dict[str, Any]) -> str:
        """Generate AI description using OpenRouter"""
        try:
            # Build context for LLM
            context = f"""
Resource: {profile['resource_name']}
Format: {profile['format']}
Dataset: {profile['dataset_title']}
Tags: {', '.join(profile['dataset_tags'])}

Dataset Description: {profile['dataset_notes'][:500] if profile['dataset_notes'] else 'Not provided'}
Resource Description: {profile['resource_description'] or 'Not provided'}
"""
            
            # Add temporal coverage
            if profile.get('temporal_coverage'):
                temp = profile['temporal_coverage']
                context += f"\n\nTemporal Coverage: {temp.get('min')} to {temp.get('max')}"
                if temp.get('year_count'):
                    context += f" ({temp['year_count']} years)"
            
            # Add column information
            if profile.get('columns_info'):
                context += "\n\nKey Columns:"
                for col_name, col_info in list(profile['columns_info'].items())[:10]:
                    context += f"\n- {col_name}: {col_info['dtype']}"
                    if col_info.get('is_geographic'):
                        context += " (geographic)"
                    elif col_info.get('numeric_stats'):
                        stats = col_info['numeric_stats']
                        context += f" (range: {stats['min']:.2f} - {stats['max']:.2f})"
            
            # Add data patterns
            if profile.get('data_patterns', {}).get('data_categories'):
                context += f"\n\nData Categories: {', '.join(profile['data_patterns']['data_categories'])}"
            
            prompt = f"""Based on the following resource information, generate a comprehensive description that will help users understand exactly what data this resource contains and how it can be used.

{context}

Generate a clear, informative description (2-3 sentences) that:
1. Clearly states what specific data this resource contains
2. Mentions the temporal coverage if available
3. Highlights key variables or metrics
4. Suggests potential use cases

Be specific and factual."""
            
            description = self._call_openrouter(prompt)
            
            if description:
                return description
            else:
                # Fallback description
                fallback = f"Resource containing {profile['resource_name']} data"
                if profile.get('temporal_coverage'):
                    temp = profile['temporal_coverage']
                    fallback += f" from {temp.get('min')} to {temp.get('max')}"
                return fallback
            
        except Exception as e:
            log.error(f"Error generating AI description: {e}")
            # Fallback description
            fallback = f"Resource containing {profile['resource_name']} data"
            if profile.get('temporal_coverage'):
                temp = profile['temporal_coverage']
                fallback += f" from {temp.get('min')} to {temp.get('max')}"
            return fallback
    
    def _create_document_content(self, profile: Dict[str, Any]) -> str:
        """Create searchable document content"""
        doc = f"""Resource: {profile['resource_name']}
Resource ID: {profile['resource_id']}
Dataset: {profile['dataset_title']}
Format: {profile['format']}

{profile['ai_description']}
"""
        
        # Add temporal coverage
        if profile.get('temporal_coverage'):
            temp = profile['temporal_coverage']
            doc += f"\n\n**TEMPORAL COVERAGE: {temp.get('min')} to {temp.get('max')}**"
            if temp.get('year_count'):
                doc += f"\nTotal Years: {temp['year_count']}"
        
        # Add column information
        if profile.get('columns_info'):
            doc += "\n\nColumns:"
            for col_name, col_info in list(profile['columns_info'].items())[:15]:
                doc += f"\n- {col_name}: {col_info['dtype']}"
                if col_info.get('unique_count'):
                    doc += f" ({col_info['unique_count']} unique values)"
        
        # Add frequency summaries for key columns
        if profile.get('frequency_summary'):
            doc += "\n\nTop Values in Key Columns:"
            for field, freq_info in list(profile['frequency_summary'].items())[:5]:
                if freq_info.get('top_values'):
                    doc += f"\n- {field}: {', '.join(freq_info['top_values'][:3])}"
        
        # Add tags
        if profile.get('dataset_tags'):
            doc += f"\n\nTags: {', '.join(profile['dataset_tags'])}"
        
        return doc
    
    def _create_metadata(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Create metadata for ChromaDB"""
        metadata = {
            'resource_id': profile['resource_id'],
            'resource_name': profile.get('resource_name', ''),
            'dataset_id': profile.get('dataset_id', ''),
            'dataset_title': profile.get('dataset_title', ''),
            'format': profile.get('format', 'unknown'),
            'has_temporal': bool(profile.get('temporal_coverage')),
            'indexed_at': datetime.now().isoformat()
        }
        
        # Add temporal metadata
        if profile.get('temporal_coverage'):
            temp = profile['temporal_coverage']
            metadata['temporal_min'] = int(temp.get('min', 0))
            metadata['temporal_max'] = int(temp.get('max', 0))
            metadata['year_count'] = int(temp.get('year_count', 0))
        else:
            metadata['temporal_min'] = 0
            metadata['temporal_max'] = 0
            metadata['year_count'] = 0
        
        # Add pattern metadata
        if profile.get('data_patterns'):
            patterns = profile['data_patterns']
            metadata['has_geographic'] = patterns.get('has_geographic', False)
            metadata['has_financial'] = patterns.get('has_financial', False)
            metadata['data_categories'] = ','.join(patterns.get('data_categories', []))
        
        return metadata
    
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