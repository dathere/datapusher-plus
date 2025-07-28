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
            self.chunk_size = conf.VECTOR_CHUNK_SIZE
            self.chunk_overlap = conf.CHUNK_OVERLAP
            
            log.info("Vector store components initialized successfully") 
            
        except Exception as e:
            log.error(f"Failed to initialize vector store: {e}")
            self.enabled = False
    
    def _split_text(self, text: str, chunk_size: int = None, chunk_overlap: int = None) -> List[str]:
        """Simple text splitter with safeguards against infinite loops"""
        if chunk_size is None:
            chunk_size = self.chunk_size
        if chunk_overlap is None:
            chunk_overlap = self.chunk_overlap
        
        log.debug(f"Text splitting - Input length: {len(text)}, chunk_size: {chunk_size}, chunk_overlap: {chunk_overlap}")
        
        # Validate inputs to prevent infinite loops
        if chunk_size <= 0:
            chunk_size = 1000  # fallback
        if chunk_overlap < 0:
            chunk_overlap = 0
        if chunk_overlap >= chunk_size:
            chunk_overlap = chunk_size // 2  # ensure overlap is less than chunk size
            log.debug(f"Adjusted chunk_overlap to {chunk_overlap} to prevent infinite loop")
            
        if len(text) <= chunk_size:
            log.debug("Text fits in single chunk")
            return [text]
        
        chunks = []
        start = 0
        max_iterations = len(text) // max(1, chunk_size - chunk_overlap) + 10  # safety limit
        iteration_count = 0
        
        log.debug(f"Starting text splitting with max_iterations: {max_iterations}")
        
        while start < len(text) and iteration_count < max_iterations:
            iteration_count += 1
            if iteration_count % 100 == 0:  # Log every 100 iterations
                log.debug(f"Text splitting iteration {iteration_count}, start position: {start}")
            
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
            
            chunk = text[start:end].strip()
            if chunk:  # only add non-empty chunks
                chunks.append(chunk)
            
            # Ensure we always advance by at least 1 character to prevent infinite loops
            next_start = end - chunk_overlap
            if next_start <= start:
                next_start = start + max(1, chunk_size - chunk_overlap)
            start = next_start
        
        # Safety check - if we hit max iterations, log a warning
        if iteration_count >= max_iterations:
            log.warning(f"Text splitting hit maximum iterations ({max_iterations}). Text length: {len(text)}")
        
        log.debug(f"Text splitting completed in {iteration_count} iterations, created {len(chunks)} chunks")
        return chunks if chunks else [text]  # ensure we always return at least the original text
    
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
            log.debug("Calling OpenRouter API...")
            response = requests.post(
                f"{self.openrouter_base_url}/chat/completions",
                headers=headers,
                json=data,
                timeout=60  # Increased timeout to 60 seconds
            )
            
            if response.status_code == 200:
                result = response.json()
                log.debug("OpenRouter API call successful")
                return result["choices"][0]["message"]["content"].strip()
            else:
                log.error(f"OpenRouter API error {response.status_code}: {response.text}")
                return ""
                
        except requests.exceptions.Timeout:
            log.error("OpenRouter API timeout after 60 seconds")
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
                      logger=None) -> bool:
        """Embed a resource after successful datastore upload"""
        
        if not self.enabled:
            return False
            
        if logger is None:
            logger = log
            
        try:
            logger.info(f"Starting vector embedding for resource {resource_id}")
            
            # Create resource profile from available data
            logger.debug("Creating resource profile...")
            profile = self._create_resource_profile(
                resource_id, resource_metadata, dataset_metadata, 
                stats_data, freq_data, temporal_info
            )
            logger.debug("Resource profile created successfully")
            
            # Generate AI description using OpenRouter
            logger.debug("Generating AI description...")
            try:
                ai_description = self._generate_ai_description(profile)
                profile['ai_description'] = ai_description
                logger.debug("AI description generated successfully")
            except Exception as e:
                logger.warning(f"Failed to generate AI description: {e}")
                profile['ai_description'] = f"Resource containing {profile['resource_name']} data"
            
            # Create document content
            logger.debug("Creating document content...")
            doc_content = self._create_document_content(profile)
            logger.debug(f"Document content created, length: {len(doc_content)} characters")
            
            # Create metadata for Pinecone
            logger.debug("Creating metadata...")
            metadata = self._create_metadata(profile)
            logger.debug(f"Metadata created with {len(metadata)} fields")
            
            # Split document if too long
            logger.debug("Splitting text into chunks...")
            try:
                chunks = self._split_text(doc_content)
                logger.info(f"Split document into {len(chunks)} chunks")
            except Exception as e:
                logger.error(f"Text splitting failed: {e}")
                # Fallback to simple chunking
                logger.debug("Using fallback chunking strategy")
                chunk_size = getattr(self, 'chunk_size', 1000)
                chunks = [doc_content[i:i+chunk_size] for i in range(0, len(doc_content), chunk_size)]
                logger.info(f"Fallback chunking created {len(chunks)} chunks")
            
            # Remove existing entries for this resource
            logger.debug("Checking for existing entries to delete...")
            try:
                # Delete existing vectors for this resource by filter
                self.index.delete(
                    filter={"resource_id": resource_id},
                    namespace=self.namespace
                )
                logger.debug("Existing entries deleted successfully")
            except Exception as e:
                # Ignore namespace not found errors on first upload
                if "Namespace not found" not in str(e):
                    logger.warning(f"Could not delete existing entries: {e}")
                else:
                    logger.debug("Namespace not found - this is expected on first upload")
            
            # Prepare data for Pinecone upsert
            logger.debug("Preparing records for Pinecone upsert...")
            records = []
            for i, chunk in enumerate(chunks):
                chunk_metadata = metadata.copy()
                chunk_metadata['chunk_index'] = int(i)
                chunk_metadata['total_chunks'] = int(len(chunks))
                
                # Create record for integrated embedding
                record = {
                    "_id": f"{resource_id}_{i}",
                    "text": chunk  # This field name must match your index's field_map
                }
                # Add metadata as separate fields (Pinecone will automatically store them as metadata)
                record.update(chunk_metadata)
                records.append(record)
            
            logger.debug(f"Prepared {len(records)} records for upsert")
            
            # Upsert to Pinecone (embeddings are generated automatically)
            logger.debug("Starting Pinecone upsert...")
            try:
                self.index.upsert_records(
                    self.namespace,  # namespace first
                    records         # then records list
                )
                logger.debug("Pinecone upsert completed successfully")
            except Exception as e:
                logger.error(f"Pinecone upsert failed: {e}")
                raise
            
            logger.info(f"Successfully embedded {len(chunks)} chunks for resource {resource_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error embedding resource {resource_id}: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def search_resources(self, query: str, n_results: int = 10, 
                        filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Search for resources using Pinecone hosted embeddings"""
        try:
            # Build search parameters for Pinecone integrated embedding
            search_params = {
                "namespace": self.namespace,
                "query": {
                    "inputs": {"text": query},
                    "top_k": n_results
                }
            }
            
            # Add metadata filters if provided
            if filters:
                search_params["query"]["filter"] = filters
            
            # Search using Pinecone
            results = self.index.search(**search_params)
            
            # Format results
            formatted_results = []
            if results and 'matches' in results:
                for match in results['matches']:
                    # For integrated embedding, the original text is in the 'text' field
                    content = match.get('text', '')
                    metadata = match.get('metadata', {})
                    
                    formatted_results.append({
                        'id': match.get('id'),
                        'content': content,
                        'metadata': metadata,
                        'score': match.get('score', 0.0),
                        'distance': 1.0 - match.get('score', 0.0)
                    })
            
            return formatted_results
            
        except Exception as e:
            log.error(f"Error searching resources: {e}")
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
            
            # Limit context length to prevent API issues
            if len(context) > 3000:
                context = context[:3000] + "..."
                log.debug("Truncated context due to length")
            
            prompt = f"""Based on the following resource information, generate a comprehensive description that will help users understand exactly what data this resource contains and how it can be used.

{context}

Generate a clear, informative description (2-3 sentences) that:
1. Clearly states what specific data this resource contains
2. Highlights key variables or metrics
3. Suggests potential use cases

Be specific and factual."""
            
            log.debug("Calling OpenRouter for AI description...")
            description = self._call_openrouter(prompt)
            
            if description:
                log.debug("AI description generated successfully")
                return description
            else:
                # Fallback description
                log.debug("OpenRouter returned empty response, using fallback")
                return f"Resource containing {profile['resource_name']} data"
            
        except Exception as e:
            log.error(f"Error generating AI description: {e}")
            # Fallback description
            return f"Resource containing {profile['resource_name']} data"
    
    def _create_document_content(self, profile: Dict[str, Any]) -> str:
        """Create searchable document content"""
        doc = f"""Resource: {profile['resource_name']}
Resource ID: {profile['resource_id']}
Dataset: {profile['dataset_title']}
Format: {profile['format']}

{profile['ai_description']}
"""
        
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
        """Create metadata for Pinecone (only simple types allowed)"""
        metadata = {
            'resource_id': str(profile['resource_id']),
            'resource_name': str(profile.get('resource_name', '')),
            'dataset_id': str(profile.get('dataset_id', '')),
            'dataset_title': str(profile.get('dataset_title', '')),
            'format': str(profile.get('format', 'unknown')),
            'indexed_at': datetime.now().isoformat()
        }
        
        # Add pattern metadata (as simple types)
        if profile.get('data_patterns'):
            patterns = profile['data_patterns']
            metadata['has_geographic'] = bool(patterns.get('has_geographic', False))
            metadata['has_financial'] = bool(patterns.get('has_financial', False))
            # Convert data categories to a single string
            categories = patterns.get('data_categories', [])
            metadata['data_categories'] = ','.join(categories) if categories else ''
        else:
            metadata['has_geographic'] = False
            metadata['has_financial'] = False
            metadata['data_categories'] = ''
        
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