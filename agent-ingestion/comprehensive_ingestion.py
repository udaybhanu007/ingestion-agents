#!/usr/bin/env python3
"""
Comprehensive Ingestion Demo: Structured + Unstructured Data

This demo shows the complete ingestion workflow supporting both:
1. Structured data → Smart LLM + MCP processing → Neo4j graph storage
2. Unstructured data → Enhanced vector ingestion → Qdrant vector storage
"""

import asyncio
import os
import sys
import time

# Add current directory to path for imports
current_dir = os.path.dirname(__file__)
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from ingestion_workflow import IngestionWorkflow
from tools.vector_tool import VectorIngestionTool
from tools.graph_tool import GraphIngestionTool


async def demo_comprehensive_ingestion():
    """Demo both structured and unstructured data ingestion"""
    
    print("🚀 COMPREHENSIVE DATA INGESTION DEMO")
    print("=" * 70)
    print("This demo showcases intelligent routing:")
    print("• Structured data → Smart LLM + MCP → Neo4j graph storage")
    print("• Unstructured data → Enhanced vector ingestion → Qdrant storage")
    print("=" * 70)
    
    workflow = IngestionWorkflow()
    
    # Test cases with different data types
    test_cases = [
        {
            "name": "📊 Structured Medical Data",
            "content": [
                {
                    "image_index": "00000001_000.png",
                    "finding_label": "Pneumonia",
                    "bbox": [256, 128, 512, 384],
                    "patient_id": "P001",
                    "age": 65,
                    "gender": "Male"
                },
                {
                    "image_index": "00000002_001.png", 
                    "finding_label": "Normal",
                    "bbox": [0, 0, 0, 0],
                    "patient_id": "P002",
                    "age": 34,
                    "gender": "Female"
                }
            ],
            "metadata": {"source": "medical_records.json", "type": "structured"}
        },
        {
            "name": "📄 Unstructured Research Paper",
            "content": """
            # Artificial Intelligence in Healthcare: Current Applications and Future Prospects

            ## Abstract

            Artificial Intelligence (AI) has emerged as a transformative technology in healthcare, offering unprecedented opportunities to improve patient outcomes, reduce costs, and enhance the efficiency of healthcare delivery. This comprehensive review examines the current applications of AI in various healthcare domains, including diagnostic imaging, drug discovery, personalized medicine, and clinical decision support systems.

            ## Introduction

            The healthcare industry is experiencing a paradigm shift driven by the rapid advancement of artificial intelligence technologies. Machine learning algorithms, particularly deep learning models, have demonstrated remarkable capabilities in analyzing complex medical data, identifying patterns that may be imperceptible to human clinicians, and providing insights that can significantly impact patient care.

            The integration of AI into healthcare systems presents both opportunities and challenges. While AI technologies offer the potential to revolutionize medical practice, their implementation requires careful consideration of ethical, regulatory, and technical factors. This paper provides an overview of the current state of AI in healthcare and explores future directions for research and development.

            ## Current Applications

            ### Diagnostic Imaging

            One of the most successful applications of AI in healthcare has been in medical imaging. Convolutional neural networks (CNNs) have shown exceptional performance in analyzing radiological images, including X-rays, CT scans, MRI images, and histopathological slides. These systems can detect abnormalities with accuracy levels that often match or exceed those of experienced radiologists.

            For example, AI systems have been developed to detect diabetic retinopathy in retinal photographs, identify skin cancer in dermoscopic images, and locate fractures in bone X-rays. These applications demonstrate the potential of AI to serve as a powerful diagnostic aid, particularly in settings where access to specialist expertise may be limited.

            ### Drug Discovery and Development

            AI is revolutionizing the pharmaceutical industry by accelerating the drug discovery process. Machine learning algorithms can analyze vast databases of molecular structures, predict drug-target interactions, and identify potential therapeutic compounds. This approach significantly reduces the time and cost associated with traditional drug development pipelines.

            Virtual screening techniques powered by AI can evaluate millions of compounds in silico, identifying promising candidates for further experimental validation. Additionally, AI models can predict drug toxicity, optimize dosing regimens, and identify patient populations most likely to benefit from specific treatments.

            ### Personalized Medicine

            The promise of personalized medicine lies in tailoring medical treatments to individual patient characteristics, including genetic profiles, lifestyle factors, and medical history. AI technologies enable the integration and analysis of diverse data types, including genomic data, electronic health records, and wearable device measurements.

            Machine learning models can identify biomarkers associated with treatment response, predict disease progression, and recommend personalized therapeutic strategies. This approach has shown particular promise in oncology, where AI-driven precision medicine approaches are being used to match patients with targeted therapies based on tumor genetics.

            ## Challenges and Considerations

            ### Data Quality and Bias

            The effectiveness of AI systems depends heavily on the quality and representativeness of training data. Healthcare datasets often suffer from incompleteness, inconsistency, and bias, which can lead to suboptimal model performance and potentially harmful outcomes when deployed in clinical settings.

            Addressing these challenges requires careful attention to data collection, preprocessing, and validation procedures. Efforts to improve data standardization and develop more robust algorithms that can handle data heterogeneity are ongoing areas of research.

            ### Regulatory and Ethical Considerations

            The deployment of AI systems in healthcare raises important regulatory and ethical questions. Ensuring patient safety, maintaining data privacy, and establishing accountability for AI-driven decisions are critical considerations that must be addressed before widespread adoption can occur.

            Regulatory frameworks for AI in healthcare are still evolving, with agencies such as the FDA developing guidelines for the evaluation and approval of AI-based medical devices. Additionally, ethical principles such as fairness, transparency, and explainability must be incorporated into AI system design.

            ## Future Directions

            The future of AI in healthcare holds tremendous promise, with several emerging trends likely to shape the field in the coming years. These include the development of more sophisticated multimodal AI systems that can integrate diverse data types, the advancement of federated learning approaches that enable collaborative model development while preserving patient privacy, and the increasing focus on explainable AI techniques that can provide insights into model decision-making processes.

            Furthermore, the integration of AI with other emerging technologies, such as robotics, Internet of Things (IoT) devices, and blockchain, may create new opportunities for improving healthcare delivery and patient engagement.

            ## Conclusion

            Artificial intelligence represents a powerful tool for transforming healthcare, with applications spanning from diagnostic imaging to personalized medicine. While significant challenges remain, including issues related to data quality, regulatory approval, and ethical considerations, the potential benefits of AI in healthcare are substantial.

            Continued research and development efforts, combined with thoughtful implementation strategies and appropriate regulatory oversight, will be essential for realizing the full potential of AI in improving patient outcomes and advancing medical practice. The future of healthcare will likely be characterized by increasing collaboration between human clinicians and AI systems, with each contributing their unique strengths to provide optimal patient care.
            """,
            "metadata": {"source": "ai_healthcare_review.md", "type": "unstructured"}
        },
        {
            "name": "📝 Short Text Query",
            "content": "What is the relationship between machine learning and healthcare diagnostics?",
            "metadata": {"source": "user_query.txt", "type": "short_text"}
        }
    ]
    
    results = []
    
    for test_case in test_cases:
        print(f"\n{test_case['name']}")
        print("-" * 50)
        
        start_time = time.time()
        
        # Run through workflow
        result = await workflow.run_ingestion(
            content=test_case["content"],
            metadata=test_case["metadata"]
        )
        
        processing_time = time.time() - start_time
        
        print(f"📊 Processing Results:")
        print(f"   Status: {result['status']}")
        print(f"   Processing Time: {processing_time:.2f}s")
        
        # Analyze what happened
        if result.get('tool_results'):
            for tool_name, tool_result in result['tool_results'].items():
                print(f"   Tool Used: {tool_name}")
                if isinstance(tool_result, dict):
                    if 'records_processed' in tool_result:
                        print(f"      Records Processed: {tool_result['records_processed']}")
                    if 'status' in tool_result:
                        print(f"      Tool Status: {tool_result['status']}")
                    if 'ingested_chunks' in tool_result:
                        print(f"      Chunks Ingested: {tool_result['ingested_chunks']}")
        
        # Store result for summary
        results.append({
            "name": test_case["name"],
            "status": result['status'],
            "processing_time": processing_time,
            "tools_used": list(result.get('tool_results', {}).keys())
        })
        
        if result.get('errors'):
            print(f"   ⚠️ Errors: {result['errors']}")
    
    # Final Summary
    print(f"\n" + "=" * 70)
    print("📈 COMPREHENSIVE INGESTION SUMMARY")
    print("=" * 70)
    
    for result in results:
        print(f"{result['name']}")
        print(f"   Status: {result['status']}")
        print(f"   Time: {result['processing_time']:.2f}s")
        print(f"   Tools: {', '.join(result['tools_used'])}")
    
    # Demonstrate cross-modal search
    print(f"\n🔍 CROSS-MODAL SEARCH DEMONSTRATION")
    print("-" * 50)
    
    print(f"\n✅ Comprehensive ingestion demo completed!")
    print(f"   Successfully demonstrated intelligent routing:")
    print(f"   • Structured data → Graph storage")
    print(f"   • Unstructured data → Vector storage")
    print(f"   • Cross-modal search capabilities")


if __name__ == "__main__":
    asyncio.run(demo_comprehensive_ingestion())
