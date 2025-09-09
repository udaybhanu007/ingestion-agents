"""
Enhanced Example Usage of the Planner Agent with Dynamic Document Sources

This example demonstrates how to use the PlannerAgent with real document sources
instead of hardcoded sample data.
"""

import asyncio
import os
from datetime import datetime
from typing import List, Dict, Any

# Import the enhanced planner agent
from agents.planner_agent import PlannerAgent

# Import configuration management
try:
    from config.document_sources import DocumentSourceConfig, DynamicSourceManager
    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False


async def main_with_dynamic_sources():
    """Main example using dynamic document sources."""
    
    print("=== Dynamic Document Processing with Planner Agent ===\n")
    
    # Initialize configuration if available
    if CONFIG_AVAILABLE:
        config = DocumentSourceConfig()
        manager = DynamicSourceManager(config)
        
        # Validate configuration
        validation = manager.validate_configuration()
        if not validation['valid']:
            print("Configuration validation failed:")
            for error in validation['errors']:
                print(f"  ERROR: {error}")
            return
        
        if validation['warnings']:
            print("Configuration warnings:")
            for warning in validation['warnings']:
                print(f"  WARNING: {warning}")
            print()
        
        # Display processing plan
        plan = manager.get_processing_plan()
        print(f"Processing Plan:")
        print(f"  Enabled sources: {plan['enabled_sources']}")
        print(f"  Total max docs: {plan['total_max_docs']}")
        print(f"  Processing order: {plan['processing_order']}")
        print()
    
    # Initialize the Planner Agent
    planner = PlannerAgent()
    
    print(f"Available connectors: {planner.get_available_connectors()}")
    print(f"LLM available: {planner.is_llm_available()}")
    print()
    
    results = []
    total_processed = 0
    
    # Process documents from each available source
    available_connectors = planner.get_available_connectors()
    
    # Azure Blob Storage
    if 'azure' in available_connectors:
        await process_azure_source(planner, results)
    
    # Box
    if 'box' in available_connectors:
        await process_box_source(planner, results)
    
    # Confluence
    if 'confluence' in available_connectors:
        await process_confluence_source(planner, results)
    
    # Fallback to sample data if no connectors available
    if not results and not available_connectors:
        print("--- No connectors available, using sample documents ---")
        results = await process_sample_documents(planner)
    
    # Display comprehensive results
    display_processing_results(results)
    
    # Save results with detailed metadata
    save_results_with_metadata(planner, results)


async def process_azure_source(planner: PlannerAgent, results: List[Dict[str, Any]]):
    """Process documents from Azure Blob Storage."""
    print("--- Processing Azure Blob Storage Documents ---")
    
    try:
        # Get configuration from environment
        container_name = os.getenv("AZURE_CONTAINER_NAME", "documents")
        prefix = os.getenv("AZURE_BLOB_PREFIX")
        max_docs = int(os.getenv("AZURE_MAX_DOCS", "10"))
        
        print(f"  Container: {container_name}")
        print(f"  Prefix: {prefix or 'None'}")
        print(f"  Max docs: {max_docs}")
        
        # Process documents
        azure_results = await planner.process_azure_documents(
            container_name=container_name,
            prefix=prefix,
            max_docs=max_docs
        )
        
        results.extend(azure_results)
        print(f"  Processed: {len(azure_results)} documents")
        
        # Show source-specific summary
        if azure_results:
            classifications = {}
            for result in azure_results:
                classification = result['classification']
                classifications[classification] = classifications.get(classification, 0) + 1
            
            print(f"  Classifications: {classifications}")
        
    except Exception as e:
        print(f"  Error processing Azure documents: {e}")
    
    print()


async def process_box_source(planner: PlannerAgent, results: List[Dict[str, Any]]):
    """Process documents from Box."""
    print("--- Processing Box Documents ---")
    
    try:
        # Get configuration from environment
        folder_id = os.getenv("BOX_FOLDER_ID", "0")
        max_docs = int(os.getenv("BOX_MAX_DOCS", "10"))
        
        print(f"  Folder ID: {folder_id}")
        print(f"  Max docs: {max_docs}")
        
        # Process documents
        box_results = await planner.process_box_documents(
            folder_id=folder_id,
            max_docs=max_docs
        )
        
        results.extend(box_results)
        print(f"  Processed: {len(box_results)} documents")
        
        # Show source-specific summary
        if box_results:
            doc_types = {}
            for result in box_results:
                doc_type = result.get('document_type', 'unknown')
                doc_types[doc_type] = doc_types.get(doc_type, 0) + 1
            
            print(f"  Document types: {doc_types}")
        
    except Exception as e:
        print(f"  Error processing Box documents: {e}")
    
    print()


async def process_confluence_source(planner: PlannerAgent, results: List[Dict[str, Any]]):
    """Process documents from Confluence."""
    print("--- Processing Confluence Documents ---")
    
    try:
        # Get page titles from environment or use defaults
        page_titles_env = os.getenv("CONFLUENCE_PAGE_TITLES", "")
        if page_titles_env:
            page_titles = [title.strip() for title in page_titles_env.split(",")]
        else:
            page_titles = [
                "API Documentation",
                "Project Overview",
                "User Guide",
                "Technical Specifications",
                "Architecture Documentation"
            ]
        
        print(f"  Page titles: {len(page_titles)} pages")
        print(f"  Pages: {', '.join(page_titles[:3])}{'...' if len(page_titles) > 3 else ''}")
        
        # Process documents
        confluence_results = await planner.process_confluence_documents(page_titles)
        
        results.extend(confluence_results)
        print(f"  Processed: {len(confluence_results)} documents")
        
        # Show source-specific summary
        if confluence_results:
            confidence_scores = [r['confidence_score'] for r in confluence_results]
            avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
            print(f"  Average confidence: {avg_confidence:.2f}")
        
    except Exception as e:
        print(f"  Error processing Confluence documents: {e}")
    
    print()


async def process_sample_documents(planner: PlannerAgent) -> List[Dict[str, Any]]:
    """Process sample documents when no connectors are available."""
    
    sample_documents = [
        {
            'uri': 'example://sample/medical_protocol.txt',
            'content': '''
            Medical Treatment Protocol for Diabetes Management
            
            1. Patient Assessment
               1.1 Blood glucose monitoring
               1.2 HbA1c testing
               1.3 Complication screening
            
            2. Treatment Plan
               2.1 Medication Management
                   - Metformin is first-line treatment
                   - Insulin therapy depends on blood glucose levels
                   - Drug interactions must be monitored
               
               2.2 Lifestyle Interventions
                   - Diet modification
                   - Exercise program
                   - Weight management
            
            3. Monitoring Protocol
               - Daily glucose checks
               - Quarterly HbA1c
               - Annual eye examination
            
            Drug Interactions:
            - Metformin interacts with contrast agents
            - Insulin dosage affects other medications
            - Monitor for hypoglycemia with combination therapy
            ''',
            'metadata': {'type': 'medical_document', 'version': '1.0'},
            'source': 'example'
        },
        {
            'uri': 'example://sample/org_chart.txt',
            'content': '''
            Company Organizational Structure
            
            CEO: John Smith
            ├── CTO: Sarah Johnson
            │   ├── VP Engineering: Mike Chen
            │   │   ├── Senior Engineer: Alice Brown
            │   │   └── Senior Engineer: Bob Wilson
            │   └── VP Product: Lisa Garcia
            │       ├── Product Manager: Tom Davis
            │       └── Product Manager: Emma Wilson
            └── CFO: David Kim
                ├── Finance Manager: Maria Rodriguez
                └── Accounting Manager: James Lee
            
            Reporting Structure:
            - All VPs report to CTO
            - All managers report to respective VPs
            - Engineers report to VP Engineering
            ''',
            'metadata': {'type': 'organizational_chart', 'department': 'hr'},
            'source': 'example'
        },
        {
            'uri': 'example://sample/research_paper.txt',
            'content': '''
            The Impact of Machine Learning on Healthcare Outcomes
            
            Abstract:
            This study examines the effectiveness of machine learning algorithms
            in predicting patient outcomes across multiple healthcare settings.
            
            Introduction:
            Healthcare systems worldwide are increasingly adopting AI technologies
            to improve patient care and reduce costs. Machine learning models
            have shown promising results in various medical applications.
            
            Methodology:
            We analyzed data from 50,000 patient records across 10 hospitals,
            using deep learning models to predict readmission rates and
            treatment effectiveness.
            
            Results:
            Our models achieved 85% accuracy in predicting 30-day readmissions
            and identified key risk factors including age, comorbidities, and
            previous hospitalization history.
            
            Conclusion:
            Machine learning can significantly improve healthcare outcomes when
            properly implemented with appropriate clinical oversight.
            ''',
            'metadata': {'type': 'research_paper', 'authors': ['Dr. Smith', 'Dr. Johnson']},
            'source': 'example'
        }
    ]
    
    print(f"Processing {len(sample_documents)} sample documents...")
    results = await planner.classify_and_plan_documents(sample_documents)
    print(f"Processed {len(results)} sample documents")
    
    return results


def display_processing_results(results: List[Dict[str, Any]]):
    """Display comprehensive processing results."""
    
    if not results:
        print("No documents were processed.")
        return
    
    print(f"=== PROCESSING COMPLETE ===")
    print(f"Total documents processed: {len(results)}")
    
    # Classification summary
    classification_summary = {}
    source_summary = {}
    confidence_scores = []
    
    for result in results:
        # Count classifications
        classification = result['classification']
        classification_summary[classification] = classification_summary.get(classification, 0) + 1
        
        # Count sources
        source = result['source']
        source_summary[source] = source_summary.get(source, 0) + 1
        
        # Collect confidence scores
        confidence_scores.append(result['confidence_score'])
    
    print(f"\nClassification Summary:")
    for classification, count in classification_summary.items():
        percentage = (count / len(results)) * 100
        print(f"  {classification}: {count} ({percentage:.1f}%)")
    
    print(f"\nSource Summary:")
    for source, count in source_summary.items():
        print(f"  {source}: {count} documents")
    
    if confidence_scores:
        avg_confidence = sum(confidence_scores) / len(confidence_scores)
        print(f"\nAverage confidence score: {avg_confidence:.2f}")
    
    # Show detailed results for first few documents
    print(f"\n--- Detailed Results (first 3) ---")
    for i, result in enumerate(results[:3], 1):
        print(f"\n{i}. Document: {result['doc_uri']}")
        print(f"   Source: {result['source']}")
        print(f"   Classification: {result['classification']}")
        print(f"   Document Type: {result['document_type']}")
        print(f"   Confidence: {result['confidence_score']:.2f}")
        print(f"   Classifier: {result['classifier_type']}")
        
        # Show key indicators
        if result.get('key_indicators'):
            indicators = ', '.join(result['key_indicators'][:3])
            print(f"   Key Indicators: {indicators}")
        
        # Show ingestion steps
        plan = result.get('ingestion_plan', {})
        steps = plan.get('steps', [])
        if steps:
            step_types = [step['action_type'] for step in steps]
            print(f"   Ingestion Steps: {', '.join(step_types)}")


def save_results_with_metadata(planner: PlannerAgent, results: List[Dict[str, Any]]):
    """Save results with comprehensive metadata."""
    
    if not results:
        print("\nNo results to save.")
        return
    
    # Create timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create enhanced results with metadata
    enhanced_results = {
        'processing_metadata': {
            'timestamp': datetime.now().isoformat(),
            'total_documents': len(results),
            'planner_version': '1.0',
            'llm_available': planner.is_llm_available(),
            'available_connectors': planner.get_available_connectors()
        },
        'classification_summary': _create_classification_summary(results),
        'documents': results
    }
    
    # Save with multiple formats
    base_filename = f"planner_results_{timestamp}"
    
    # Save detailed JSON
    detailed_file = f"{base_filename}_detailed.json"
    if planner.save_results([enhanced_results], detailed_file):
        print(f"\nDetailed results saved to: {detailed_file}")
    
    # Save summary JSON (just the documents)
    summary_file = f"{base_filename}_summary.json"
    if planner.save_results(results, summary_file):
        print(f"Summary results saved to: {summary_file}")
    
    # Save CSV summary
    csv_file = f"{base_filename}_summary.csv"
    _save_csv_summary(results, csv_file)
    print(f"CSV summary saved to: {csv_file}")


def _create_classification_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create classification summary statistics."""
    
    summary = {
        'total_documents': len(results),
        'classifications': {},
        'sources': {},
        'document_types': {},
        'confidence_stats': {}
    }
    
    confidence_scores = []
    
    for result in results:
        # Classifications
        classification = result['classification']
        summary['classifications'][classification] = summary['classifications'].get(classification, 0) + 1
        
        # Sources
        source = result['source']
        summary['sources'][source] = summary['sources'].get(source, 0) + 1
        
        # Document types
        doc_type = result.get('document_type', 'unknown')
        summary['document_types'][doc_type] = summary['document_types'].get(doc_type, 0) + 1
        
        # Confidence scores
        confidence_scores.append(result['confidence_score'])
    
    # Confidence statistics
    if confidence_scores:
        summary['confidence_stats'] = {
            'average': sum(confidence_scores) / len(confidence_scores),
            'minimum': min(confidence_scores),
            'maximum': max(confidence_scores),
            'high_confidence_count': len([c for c in confidence_scores if c >= 0.8]),
            'low_confidence_count': len([c for c in confidence_scores if c < 0.5])
        }
    
    return summary


def _save_csv_summary(results: List[Dict[str, Any]], filename: str):
    """Save results summary as CSV."""
    
    try:
        import csv
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'doc_uri', 'source', 'classification', 'document_type', 
                'confidence_score', 'classifier_type', 'ingestion_steps'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                # Get ingestion steps
                plan = result.get('ingestion_plan', {})
                steps = plan.get('steps', [])
                step_types = [step['action_type'] for step in steps]
                
                writer.writerow({
                    'doc_uri': result['doc_uri'],
                    'source': result['source'],
                    'classification': result['classification'],
                    'document_type': result['document_type'],
                    'confidence_score': result['confidence_score'],
                    'classifier_type': result['classifier_type'],
                    'ingestion_steps': ', '.join(step_types)
                })
        
    except ImportError:
        print("CSV module not available, skipping CSV export")
    except Exception as e:
        print(f"Error saving CSV: {e}")


if __name__ == "__main__":
    asyncio.run(main_with_dynamic_sources())
