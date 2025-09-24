#!/usr/bin/env python3
"""
Test Box file 1969320109971 ingestion with Qdrant cloud and comprehensive SSL fixes
"""

import os
import sys
import ssl
import asyncio
import warnings
import json

# Comprehensive SSL fixes for Qdrant cloud
os.environ['PYTHONHTTPSVERIFY'] = '0'
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''

# Disable SSL warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Apply SSL context fixes early
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except:
    pass

# Monkey patch httpx for Qdrant cloud connection
def patch_httpx_ssl():
    try:
        import httpx
        from httpx._config import create_ssl_context
        
        def patched_create_ssl_context(*args, **kwargs):
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            return context
        
        httpx._config.create_ssl_context = patched_create_ssl_context
        print("✅ HTTPX SSL context patched for Qdrant cloud")
    except Exception as e:
        print(f"⚠️ HTTPX patch failed: {e}")

# Apply patches early
patch_httpx_ssl()

async def test_box_file_ingestion():
    """Test ingestion of Box file 1969320109971 with Qdrant cloud"""
    
    # Load environment to get collection name
    from dotenv import load_dotenv
    load_dotenv('.env.dev')
    
    collection_name = os.getenv('QDRANT_COLLECTION', 'agent_research_doc')
    
    print("🚀 Testing Box file ingestion with Qdrant cloud...")
    print(f"📦 Box File ID: 1969320109971")
    print(f"☁️ Qdrant Cloud: https://f779f36d-3ee0-4afe-b35c-9ced9a62f083.us-west-1-0.aws.cloud.qdrant.io")
    print(f"📁 Collection: {collection_name}")
    print("-" * 60)

    try:
        # Import with SSL fixes applied
        from agents.planner_agent import PlannerAgent
        from agents.execution_agent import ExecutionAgent

        doc_uri = "azure://rag-agents-container/Data_Entry_2017.csv"
        metadata = {
            "document_source": "box",
            "document_type": "txt",
            "content_type": "text/plain",
            "processing_options": {},
            "test_type": "graph_only"  # Focus on graph ingestion for nodes and relationships
        }


        # Simple vector test content with paragraph breaks for chunking
        # sample_content = (
        # "NIH Chest X-ray Dataset of 14 Common Thorax Disease Categories:\n\n"
        # "(1, Atelectasis; 2, Cardiomegaly; 3, Effusion; 4, Infiltration; 5, Mass; 6, Nodule; 7, Pneumonia; 8, Pneumothorax; 9, Consolidation; 10, Edema; 11, Emphysema; 12, Fibrosis; 13, Pleural_Thickening; 14 Hernia)\n\n"
        # "Background & Motivation: Chest X-ray exam is one of the most frequent and cost-effective medical imaging examination. However clinical diagnosis of chest X-ray can be challenging, and sometimes believed to be harder than diagnosis via chest CT imaging.\n\n"
        # "Even some promising work have been reported in the past, and especially in recent deep learning work on Tuberculosis (TB) classification. To achieve clinically relevant computer-aided detection and diagnosis (CAD) in real world medical sites on all data settings of chest X-rays is still very difficult, if not impossible when only several thousands of images are employed for study.\n\n"
        # "This is evident from [2] where the performance deep neural networks for thorax disease recognition is severely limited by the availability of only 4143 frontal view images [3] (Openi is the previous largest publicly available chest X-ray dataset to date).\n\n"
        # "In this database, we provide an enhanced version (with 6 more disease categories and more images as well) of the dataset used in the recent work [1] which is approximately 27 times of the number of frontal chest x-ray images in [3]. Our dataset is extracted from the clinical PACS database at National Institutes of Health Clinical Center and consists of ~60% of all frontal chest x-rays in the hospital.\n\n"
        # "Therefore we expect this dataset is significantly more representative to the real patient population distributions and realistic clinical diagnosis challenges, than any previous chest x-ray datasets. Of course, the size of our dataset, in terms of the total numbers of images and thorax disease frequencies, would better facilitate deep neural network training [2].\n\n"
        # "Refer to [1] on the details of how the dataset is extracted and image labels are mined through natural language processing (NLP).\n\n"
        # "Details: ChestX-ray dataset comprises 112,120 frontal-view X-ray images of 30,805 unique patients with the text-mined fourteen disease image labels (where each image can have multi-labels), mined from the associated radiological reports using natural language processing.\n\n"
        # "Fourteen common thoracic pathologies include Atelectasis, Consolidation, Infiltration, Pneumothorax, Edema, Emphysema, Fibrosis, Effusion, Pneumonia, Pleural_thickening, Cardiomegaly, Nodule, Mass and Hernia, which is an extension of the 8 common disease patterns listed in our CVPR 2017 paper.\n\n"
        # "Note that original radiology reports (associated with these chest x-ray studies) are not meant to be publicly shared for many reasons. The text-mined disease labels are expected to have accuracy >90%. Please find more details and benchmark performance of trained models based on 14 disease labels in our arxiv paper: 1705.02315.\n\n"
        # "Contents:\n\n"
        # "1. 112,120 frontal-view chest X-ray PNG images in 1024*1024 resolution (under images folder)\n\n"
        # "2. Meta data for all images (Data_Entry_2017.csv): Image Index, Finding Labels, Follow-up #, Patient ID, Patient Age, Patient Gender, View Position, Original Image Size and Original Image Pixel Spacing.\n\n"
        # "3. Bounding boxes for ~1000 images (BBox_List_2017.csv): Image Index, Finding Label, Bbox[x, y, w, h]. [x y] are coordinates of each box's topleft corner. [w h] represent the width and height of each box.\n\n"
        # "4. Two data split files (train_val_list.txt and test_list.txt) are provided. Images in the ChestX-ray dataset are divided into these two sets on the patient level. All studies from the same patient will only appear in either training/validation or testing set.\n\n"
        # "If you find the dataset useful for your research projects, please cite our CVPR 2017 paper: Xiaosong Wang, Yifan Peng, Le Lu, Zhiyong Lu, Mohammadhadi Bagheri, Ronald M. Summers. ChestX-ray8: Hospital-scale Chest X-ray Database and Benchmarks on Weakly-Supervised Classification and Localization of Common Thorax Diseases, IEEE CVPR, pp. 3462-3471,2017.\n\n"
        # "Questions Comments: (xiaosong.wang@nih.gov; le.lu@nih.gov; rms@nih.gov)\n\n"
        # "Limitations: 1) The image labels are NLP extracted so there would be some erroneous labels but the NLP labelling accuracy is estimated to be >90%. 2) Very limited numbers of disease region bounding boxes. 3) Chest x-ray radiology reports are not anticipated to be publicly shared. Parties who use this public dataset are encouraged to share their 'updated' image labels and/or new bounding boxes in their own studied later, maybe through manual annotation.\n\n"
        # "Acknowledgement: This work was supported by the Intramural Research Program of the NIH Clinical Center (clinicalcenter.nih.gov) and National Library of Medicine (www.nlm.nih.gov). We thank NVIDIA Corporation for the GPU donations.\n\n"
        # "Reference: [1] Xiaosong Wang, Yifan Peng, Le Lu, Zhiyong Lu, Mohammadhadi Bagheri, Ronald Summers, ChestX-ray8: Hospital-scale Chest X-ray Database and Benchmarks on Weakly-Supervised Classification and Localization of Common Thorax Diseases, IEEE CVPR, pp. 3462-3471, 2017. [2] Hoo-chang Shin, Kirk Roberts, Le Lu, Dina Demner-Fushman, Jianhua Yao, Ronald M. Summers, Learning to Read Chest X-Rays: Recurrent Neural Cascade Model for Automated Image Annotation, IEEE CVPR, pp. 2497-2506, 2016."
        # )

        print("🔧 Initializing planner and executor agents...")
        planner = PlannerAgent()
        executor = ExecutionAgent()

        print("📋 Creating ingestion plan...")
        plan = await planner.create_ingestion_plan_async(
            doc_uri=doc_uri,
            metadata=metadata
            #content=""
        )

        # Print the plan in JSON format (use repr fallback for non-serializable objects)
        try:
            print("📦 Ingestion plan (JSON):")
            print(json.dumps(plan, indent=2, default=lambda o: repr(o)))
        except Exception as e:
            print(f"⚠️ Failed to serialize plan to JSON: {e}")
            print("Raw plan:", repr(plan))

        print("⚡ Executing ingestion plan...")
        result = await executor.execute_plan_async(plan)

        print("🎉 Ingestion completed!")
        print(f"📈 Result: {result}")

        return result

    except Exception as e:
        print(f"❌ Error during ingestion: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function to run the test"""
    print("=" * 60)
    print("BOX FILE INGESTION TEST - QDRANT CLOUD")
    print("=" * 60)
    
    result = asyncio.run(test_box_file_ingestion())
    
    if result:
        print("\n✅ SUCCESS: Box file ingestion completed!")
        print("🔍 Check your Qdrant cloud dashboard for the ingested vectors")
        print("📊 Collection: agent_research_doc")
        print("🌐 Dashboard: https://cloud.qdrant.io")
    else:
        print("\n❌ FAILED: Box file ingestion failed")
        print("🔧 Check SSL configuration and Qdrant cloud connectivity")

if __name__ == "__main__":
    main()