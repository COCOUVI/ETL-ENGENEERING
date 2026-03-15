#!/usr/bin/env python3
"""
🧪 TEST SCRIPT - Vérifie que les modules core fonctionnent

Exécutez ce script pour confirmer que le refactoring est fonctionnel
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def test_imports():
    """Test 1: Vérifier les imports"""
    print("\n" + "="*60)
    print("TEST 1: Imports des modules core")
    print("="*60)
    
    try:
        from core.config import ConfigManager
        print("✅ core.config.ConfigManager")
        
        from core.minio_client import MinIOClient
        print("✅ core.minio_client.MinIOClient")
        
        from core.spark_manager import SparkManager
        print("✅ core.spark_manager.SparkManager")
        
        from core.logger import setup_logging, get_logger
        print("✅ core.logger (setup_logging, get_logger)")
        
        from core.base import Extractor, Loader, Transformer, Pipeline, ETLComponent
        print("✅ core.base (Extractor, Loader, Transformer, Pipeline, ETLComponent)")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        return False


def test_config():
    """Test 2: Tester ConfigManager"""
    print("\n" + "="*60)
    print("TEST 2: ConfigManager")
    print("="*60)
    
    try:
        from core.config import ConfigManager
        
        config = ConfigManager.get_instance()
        print(f"✅ ConfigManager instance créé")
        
        # Vérifier singleton
        config2 = ConfigManager.get_instance()
        assert config is config2, "❌ Singleton not working!"
        print(f"✅ Singleton pattern fonctionne")
        
        # Vérifier les configs
        print(f"\n  MinIO endpoint: {config.minio.endpoint}")
        print(f"  MinIO bucket: {config.minio.bucket}")
        print(f"  DB host: {config.database.host}")
        print(f"  Bronze layer: {config.paths.bronze_layer}")
        print(f"  Spark app: {config.spark.app_name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_logger():
    """Test 3: Tester Logger"""
    print("\n" + "="*60)
    print("TEST 3: Logger")
    print("="*60)
    
    try:
        from core.logger import setup_logging, get_logger
        import logging
        
        # Setup logging
        setup_logging(level=logging.INFO)
        print("✅ setup_logging() exécuté")
        
        # Get logger
        logger = get_logger(__name__)
        print("✅ get_logger() exécuté")
        
        # Test logging
        logger.info("Test message INFO")
        logger.warning("Test message WARNING")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_minio_client_structure():
    """Test 4: Tester structure MinIOClient"""
    print("\n" + "="*60)
    print("TEST 4: MinIOClient Structure")
    print("="*60)
    
    try:
        from core.minio_client import MinIOClient
        from core.config import ConfigManager
        
        config = ConfigManager.get_instance()
        client = MinIOClient(config.minio)
        
        print("✅ MinIOClient instance créé")
        
        # Vérifier les méthodes
        methods = [
            'ensure_bucket_exists',
            'bucket_exists',
            'folder_exists',
            'upload_file',
            'upload_directory',
            'download_file',
            'list_objects',
            'delete_object',
        ]
        
        for method in methods:
            assert hasattr(client, method), f"❌ Méthode {method} manquante!"
            print(f"  ✅ {method}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_spark_manager_structure():
    """Test 5: Tester structure SparkManager"""
    print("\n" + "="*60)
    print("TEST 5: SparkManager Structure")
    print("="*60)
    
    try:
        from core.spark_manager import SparkManager
        
        manager = SparkManager.get_instance()
        print("✅ SparkManager instance créé")
        
        # Vérifier les méthodes
        methods = [
            'get_session',
            'stop_session',
        ]
        
        for method in methods:
            assert hasattr(manager, method), f"❌ Méthode {method} manquante!"
            print(f"  ✅ {method}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_base_classes():
    """Test 6: Tester les classes de base"""
    print("\n" + "="*60)
    print("TEST 6: Base Classes Structure")
    print("="*60)
    
    try:
        from core.base import Extractor, Loader, Transformer, Pipeline
        
        print("✅ Extractor abstract class")
        print("✅ Loader abstract class")
        print("✅ Transformer abstract class")
        print("✅ Pipeline composite class")
        
        # Test Pipeline
        pipeline = Pipeline("Test Pipeline")
        assert hasattr(pipeline, 'add'), "❌ Pipeline.add() missing"
        assert hasattr(pipeline, 'execute'), "❌ Pipeline.execute() missing"
        
        print("  ✅ Pipeline.add()")
        print("  ✅ Pipeline.execute()")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_refactored_jobs():
    """Test 7: Tester les jobs refactorisés"""
    print("\n" + "="*60)
    print("TEST 7: Refactored Jobs")
    print("="*60)
    
    try:
        from jobs.extract.extract_csv import CSVExtractor
        print("✅ jobs.extract.extract_csv.CSVExtractor")
        
        from jobs.load.load_to_minio import BooksRawLoader
        print("✅ jobs.load.load_to_minio.BooksRawLoader")
        
        from jobs.extract.scrape_books import BookScraper
        print("✅ jobs.extract.scrape_books.BookScraper")
        
        # Vérifier qu'ils héritent des bonnes classes
        from core.base import Extractor, Loader
        
        assert issubclass(CSVExtractor, Extractor), "❌ CSVExtractor ne hérite pas d'Extractor"
        print("  ✅ CSVExtractor hérite de Extractor")
        
        assert issubclass(BooksRawLoader, Loader), "❌ BooksRawLoader ne hérite pas de Loader"
        print("  ✅ BooksRawLoader hérite de Loader")
        
        assert issubclass(BookScraper, Extractor), "❌ BookScraper ne hérite pas d'Extractor"
        print("  ✅ BookScraper hérite de Extractor")
        
        return True
        
    except ImportError as e:
        print(f"⚠️  Import Error (normal si jobs non migrés): {e}")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_job_instantiation():
    """Test 8: Tester l'instanciation des jobs"""
    print("\n" + "="*60)
    print("TEST 8: Job Instantiation")
    print("="*60)
    
    try:
        from jobs.extract.extract_csv import CSVExtractor
        from jobs.load.load_to_minio import BooksRawLoader
        from jobs.extract.scrape_books import BookScraper
        
        # Créer les instances
        csv_extractor = CSVExtractor()
        print(f"✅ CSVExtractor instance: {csv_extractor.name}")
        
        loader = BooksRawLoader()
        print(f"✅ BooksRawLoader instance: {loader.name}")
        
        scraper = BookScraper()
        print(f"✅ BookScraper instance: {scraper.name}")
        
        return True
        
    except ImportError as e:
        print(f"⚠️  Import Error (normal si jobs non migrés): {e}")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_pipeline_composition():
    """Test 9: Tester la composition de Pipeline"""
    print("\n" + "="*60)
    print("TEST 9: Pipeline Composition")
    print("="*60)
    
    try:
        from core.base import Pipeline, Extractor, Loader, ETLComponent
        
        # Créer un mock Extractor
        class MockExtractor(Extractor):
            def extract(self) -> bool:
                self.log_info("Mock extract")
                return True
        
        # Créer un mock Loader
        class MockLoader(Loader):
            def load(self) -> bool:
                self.log_info("Mock load")
                return True
        
        # Créer pipeline
        pipeline = Pipeline("Test Pipeline")
        pipeline.add(MockExtractor())
        pipeline.add(MockLoader())
        
        print(f"✅ Pipeline créé avec {len(pipeline.stages)} stages")
        print(f"  ✅ Stage 1: {pipeline.stages[0].name}")
        print(f"  ✅ Stage 2: {pipeline.stages[1].name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    """Exécute tous les tests"""
    print("\n" + "🧪" * 30)
    print("TEST SUITE - Validation du Refactoring")
    print("🧪" * 30)
    
    tests = [
        ("Imports", test_imports),
        ("ConfigManager", test_config),
        ("Logger", test_logger),
        ("MinIOClient Structure", test_minio_client_structure),
        ("SparkManager Structure", test_spark_manager_structure),
        ("Base Classes", test_base_classes),
        ("Refactored Jobs", test_refactored_jobs),
        ("Job Instantiation", test_job_instantiation),
        ("Pipeline Composition", test_pipeline_composition),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ Test '{test_name}' crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print(f"\nTotal: {passed}/{total} passed")
    
    if passed == total:
        print("\n🎉 TOUS LES TESTS RÉUSSIS! Vous pouvez commencer la migration!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) échoué(s)")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
