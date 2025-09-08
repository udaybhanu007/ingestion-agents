#!/usr/bin/env python3
"""
Simple Unstructured Data Vector Ingestion Demo

This demo specifically shows how unstructured text gets processed 
through the enhanced vector ingestion pipeline.
"""

import asyncio
import time
from tools.vector_tool import VectorIngestionTool
from document_chunker import chunk_text_content


async def demo_unstructured_ingestion():
    """Simple unstructured data ingestion demo"""
    
    print("📄 UNSTRUCTURED DATA VECTOR INGESTION DEMO")
    print("=" * 55)
    
    # Sample unstructured research paper content
    research_paper = """
    # Machine Learning in Medical Diagnosis: A Systematic Review

    ## Abstract

    Machine learning (ML) has emerged as a powerful tool in medical diagnosis, offering the potential to improve accuracy, efficiency, and accessibility of healthcare services. This systematic review examines recent advances in ML applications for medical diagnosis across various medical specialties.

    ## Introduction

    The integration of artificial intelligence and machine learning into healthcare represents one of the most significant technological advances in modern medicine. Traditional diagnostic methods, while effective, often rely heavily on subjective interpretation and can be time-consuming and prone to human error.

    Machine learning algorithms, particularly deep learning models, have demonstrated remarkable capabilities in pattern recognition and data analysis. These technologies can process vast amounts of medical data, including imaging studies, laboratory results, and electronic health records, to identify subtle patterns that may not be immediately apparent to human clinicians.

    ## Methodology

    This systematic review follows the PRISMA guidelines for systematic reviews and meta-analyses. We conducted a comprehensive literature search across multiple databases including PubMed, IEEE Xplore, and Google Scholar. The search strategy included terms related to machine learning, artificial intelligence, medical diagnosis, and healthcare applications.

    Inclusion criteria encompassed peer-reviewed articles published between 2020 and 2024 that reported on the application of machine learning techniques in medical diagnosis. Studies were required to include performance metrics and validation procedures for the proposed ML models.

    ## Results

    ### Radiology and Medical Imaging

    Computer vision and deep learning have shown exceptional performance in medical image analysis. Convolutional neural networks (CNNs) have been successfully applied to detect abnormalities in various imaging modalities including X-rays, CT scans, MRI, and ultrasound.

    Notable achievements include automated detection of pneumonia in chest X-rays with sensitivity and specificity exceeding 90%, identification of diabetic retinopathy in retinal photographs, and early detection of cancer in mammography screening programs.

    ### Pathology

    Digital pathology has benefited significantly from ML applications. Whole slide image analysis using deep learning has enabled automated detection and classification of cancer cells, assessment of tumor grades, and prediction of treatment responses.

    Machine learning models have demonstrated the ability to identify morphological features in tissue samples that correlate with genetic mutations and molecular subtypes, providing valuable information for personalized treatment planning.

    ### Cardiology

    Electrocardiogram (ECG) analysis using machine learning has shown promise in detecting cardiac arrhythmias, predicting heart failure, and identifying patients at risk for sudden cardiac death. Automated ECG interpretation systems can process large volumes of data and provide rapid, accurate assessments.

    Additionally, ML algorithms applied to echocardiography and cardiac MRI have improved the accuracy of cardiac function assessment and structural abnormality detection.

    ## Discussion

    The results of this systematic review demonstrate the significant potential of machine learning in medical diagnosis. However, several challenges and limitations must be addressed before widespread clinical implementation can occur.

    ### Technical Challenges

    Data quality and standardization remain significant obstacles. Medical datasets often suffer from inconsistencies in data collection, labeling, and preprocessing. Additionally, the "black box" nature of many ML algorithms raises concerns about interpretability and clinical decision-making.

    ### Regulatory and Ethical Considerations

    The deployment of ML systems in clinical practice requires rigorous validation and regulatory approval. Issues related to bias, fairness, and patient privacy must be carefully considered during system development and implementation.

    ### Clinical Integration

    Successful integration of ML tools into clinical workflows requires consideration of user experience, workflow optimization, and training programs for healthcare professionals. The technology must complement rather than replace clinical expertise.

    ## Future Directions

    Future research should focus on developing more robust and interpretable ML models, establishing standardized evaluation frameworks, and conducting large-scale clinical validation studies. Federated learning approaches may address data privacy concerns while enabling collaborative model development across institutions.

    The integration of multimodal data sources, including genomics, proteomics, and environmental factors, promises to advance personalized medicine and improve diagnostic accuracy further.

    ## Conclusion

    Machine learning represents a transformative technology in medical diagnosis with demonstrated capabilities across multiple medical specialties. While challenges remain, continued research and development efforts, combined with appropriate regulatory oversight and clinical validation, will be essential for realizing the full potential of ML in improving patient outcomes and advancing medical practice.

    The future of medical diagnosis will likely involve increasing collaboration between human clinicians and AI systems, leveraging the strengths of both to provide optimal patient care and improve healthcare delivery worldwide.
    """
    
    try:
        print("📊 Content Analysis:")
        print(f"   Length: {len(research_paper):,} characters")
        print(f"   Words: {len(research_paper.split()):,} words") 
        print(f"   Paragraphs: {research_paper.count('## ') + research_paper.count('### ')}")
        
        # Test different chunking approaches
        print(f"\n🔧 Testing Chunking Strategies:")
        
        chunking_configs = [
            {"name": "Conservative", "chunk_size": 2, "overlap": 1, "max_words": 200},
            {"name": "Balanced", "chunk_size": 3, "overlap": 1, "max_words": 300},
            {"name": "Comprehensive", "chunk_size": 4, "overlap": 2, "max_words": 400}
        ]
        
        for config in chunking_configs:
            chunks = chunk_text_content(
                content=research_paper,
                source_file="ml_medical_diagnosis_review.md",
                chunk_size=config["chunk_size"],
                overlap=config["overlap"],
                max_words=config["max_words"]
            )
            
            word_counts = [chunk['metadata']['word_count'] for chunk in chunks]
            avg_words = sum(word_counts) / len(word_counts) if word_counts else 0
            
            print(f"   {config['name']:12} → {len(chunks):2d} chunks, avg {avg_words:5.1f} words")
        
        # Use balanced approach for actual ingestion
        print(f"\n🔗 Vector Ingestion Process:")
        
        vector_tool = VectorIngestionTool()
        metadata = {
            "source": "ml_medical_diagnosis_review.md",
            "document_type": "research_paper",
            "domain": "medical_machine_learning",
            "content_type": "unstructured_text",
            "author": "systematic_review",
            "year": "2024"
        }
        
        start_time = time.time()
        result = await vector_tool.ingest(research_paper, metadata)
        ingestion_time = time.time() - start_time
        
        print(f"   Status: {result.get('status', 'unknown')}")
        print(f"   Chunks Created: {result.get('ingested_chunks', 0)}")
        print(f"   Collection: {result.get('collection', 'unknown')}")
        print(f"   Ingestion Time: {ingestion_time:.2f}s")
        
        if result.get('status') != 'success':
            print(f"   Error: {result.get('error', 'Unknown error')}")
            return
        
        # Test semantic search capabilities
        print(f"\n🔍 Semantic Search Testing:")
        
        test_queries = [
            "machine learning applications in radiology",
            "challenges in medical AI implementation", 
            "deep learning for pathology diagnosis",
            "future directions in medical machine learning",
            "ECG analysis using artificial intelligence"
        ]
        
        for query in test_queries:
            print(f"\n   Query: '{query}'")
            
            search_results = await vector_tool.search(
                query=query,
                limit=2,
                score_threshold=0.65
            )
            
            if search_results:
                for i, result in enumerate(search_results, 1):
                    score = result['score']
                    content = result['content'][:150].replace('\n', ' ').strip()
                    print(f"      {i}. Score: {score:.3f}")
                    print(f"         Match: {content}...")
            else:
                print("      No relevant matches found")
        
        # Demonstrate content retrieval by topic
        print(f"\n📚 Topic-Based Content Retrieval:")
        
        topics = ["methodology", "results", "conclusion"]
        
        for topic in topics:
            topic_results = await vector_tool.search(
                query=f"{topic} section content",
                limit=1,
                score_threshold=0.5
            )
            
            if topic_results:
                content = topic_results[0]['content'][:200].replace('\n', ' ')
                print(f"   {topic.capitalize():12}: {content}...")
        
        print(f"\n✅ Unstructured Vector Ingestion Demo Complete!")
        print(f"   • Successfully processed {len(research_paper.split())} words")
        print(f"   • Created {result.get('ingested_chunks', 0)} searchable chunks")
        print(f"   • Demonstrated semantic search capabilities")
        print(f"   • Validated topic-based content retrieval")
        
    except Exception as e:
        print(f"\n❌ Demo failed: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(demo_unstructured_ingestion())
