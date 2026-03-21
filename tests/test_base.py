"""Tests for core.base module"""

import pytest
import logging
from unittest.mock import MagicMock, patch
from core.base import (
    ETLComponent, Extractor, Transformer, Loader, DataSource, Pipeline
)


class ConcreteETLComponent(ETLComponent):
    """Concrete implementation of ETLComponent for testing"""
    
    def __init__(self, name="TestComponent", should_succeed=True):
        super().__init__(name=name)
        self.should_succeed = should_succeed
        self.execute_called = False
    
    def execute(self) -> bool:
        self.execute_called = True
        if self.should_succeed:
            self.log_info("Test execution successful")
            return True
        else:
            self.log_error("Test execution failed")
            return False


class ConcreteExtractor(Extractor):
    """Concrete implementation of Extractor for testing"""
    
    def __init__(self, name="TestExtractor", should_succeed=True):
        super().__init__(name=name)
        self.should_succeed = should_succeed
    
    def extract(self) -> bool:
        if self.should_succeed:
            self.log_info("Extract successful")
            return True
        else:
            self.log_error("Extract failed")
            return False


class ConcreteTransformer(Transformer):
    """Concrete implementation of Transformer for testing"""
    
    def __init__(self, name="TestTransformer", should_succeed=True):
        super().__init__(name=name)
        self.should_succeed = should_succeed
    
    def transform(self) -> bool:
        if self.should_succeed:
            self.log_info("Transform successful")
            return True
        else:
            self.log_error("Transform failed")
            return False


class ConcreteLoader(Loader):
    """Concrete implementation of Loader for testing"""
    
    def __init__(self, name="TestLoader", should_succeed=True):
        super().__init__(name=name)
        self.should_succeed = should_succeed
    
    def load(self) -> bool:
        if self.should_succeed:
            self.log_info("Load successful")
            return True
        else:
            self.log_error("Load failed")
            return False


class ConcreteDataSource(DataSource):
    """Concrete implementation of DataSource for testing"""
    
    def __init__(self, data=None):
        self.data = data or {}
    
    def read(self):
        return self.data
    
    def validate(self) -> bool:
        return bool(self.data)
    
    @property
    def source_name(self) -> str:
        return "TestSource"


class TestETLComponent:
    """Test ETLComponent abstract class"""

    def test_etl_component_cannot_be_instantiated(self):
        """Test ETLComponent cannot be directly instantiated"""
        with pytest.raises(TypeError):
            ETLComponent(name="Test")

    def test_concrete_component_initialization(self):
        """Test concrete ETLComponent initialization"""
        component = ConcreteETLComponent(name="TestComp")
        assert component.name == "TestComp"
        assert component.logger is not None

    def test_component_logging_methods(self):
        """Test component logging methods exist and work"""
        component = ConcreteETLComponent()
        
        # Should not raise exceptions
        component.log_info("Info message")
        component.log_error("Error message")
        component.log_warning("Warning message")

    def test_component_execute_success(self):
        """Test component execute returns True on success"""
        component = ConcreteETLComponent(should_succeed=True)
        result = component.execute()
        assert result is True
        assert component.execute_called is True

    def test_component_execute_failure(self):
        """Test component execute returns False on failure"""
        component = ConcreteETLComponent(should_succeed=False)
        result = component.execute()
        assert result is False


class TestExtractor:
    """Test Extractor class"""

    def test_extractor_cannot_be_instantiated(self):
        """Test Extractor cannot be directly instantiated"""
        with pytest.raises(TypeError):
            Extractor(name="Test")

    def test_extractor_execute_calls_extract(self):
        """Test Extractor.execute() calls extract()"""
        extractor = ConcreteExtractor(should_succeed=True)
        result = extractor.execute()
        assert result is True

    def test_extractor_execute_handles_extract_failure(self):
        """Test Extractor.execute() handles extract failure"""
        extractor = ConcreteExtractor(should_succeed=False)
        result = extractor.execute()
        assert result is False

    def test_extractor_execute_with_exception(self):
        """Test Extractor.execute() when extract raises exception"""
        extractor = ConcreteExtractor()
        with patch.object(extractor, 'extract', side_effect=RuntimeError("Extract error")):
            with pytest.raises(RuntimeError):
                extractor.execute()

    def test_extractor_has_extract_method(self):
        """Test Extractor has abstract extract method"""
        assert hasattr(Extractor, 'extract')


class TestTransformer:
    """Test Transformer class"""

    def test_transformer_cannot_be_instantiated(self):
        """Test Transformer cannot be directly instantiated"""
        with pytest.raises(TypeError):
            Transformer(name="Test")

    def test_transformer_execute_calls_transform(self):
        """Test Transformer.execute() calls transform()"""
        transformer = ConcreteTransformer(should_succeed=True)
        result = transformer.execute()
        assert result is True

    def test_transformer_execute_handles_transform_failure(self):
        """Test Transformer.execute() handles transform failure"""
        transformer = ConcreteTransformer(should_succeed=False)
        result = transformer.execute()
        assert result is False

    def test_transformer_execute_with_exception(self):
        """Test Transformer.execute() when transform raises exception"""
        transformer = ConcreteTransformer()
        with patch.object(transformer, 'transform', side_effect=RuntimeError("Transform error")):
            with pytest.raises(RuntimeError):
                transformer.execute()


class TestLoader:
    """Test Loader class"""

    def test_loader_cannot_be_instantiated(self):
        """Test Loader cannot be directly instantiated"""
        with pytest.raises(TypeError):
            Loader(name="Test")

    def test_loader_execute_calls_load(self):
        """Test Loader.execute() calls load()"""
        loader = ConcreteLoader(should_succeed=True)
        result = loader.execute()
        assert result is True

    def test_loader_execute_handles_load_failure(self):
        """Test Loader.execute() handles load failure"""
        loader = ConcreteLoader(should_succeed=False)
        result = loader.execute()
        assert result is False

    def test_loader_execute_with_exception(self):
        """Test Loader.execute() when load raises exception"""
        loader = ConcreteLoader()
        with patch.object(loader, 'load', side_effect=RuntimeError("Load error")):
            with pytest.raises(RuntimeError):
                loader.execute()


class TestDataSource:
    """Test DataSource abstract class"""

    def test_datasource_cannot_be_instantiated(self):
        """Test DataSource cannot be directly instantiated"""
        with pytest.raises(TypeError):
            DataSource()

    def test_concrete_datasource_read(self):
        """Test concrete DataSource read method"""
        data = {"key": "value"}
        source = ConcreteDataSource(data=data)
        assert source.read() == data

    def test_concrete_datasource_validate(self):
        """Test concrete DataSource validate method"""
        source = ConcreteDataSource(data={"key": "value"})
        assert source.validate() is True

    def test_concrete_datasource_validate_empty(self):
        """Test DataSource validate with empty data"""
        source = ConcreteDataSource(data={})
        assert source.validate() is False

    def test_concrete_datasource_source_name(self):
        """Test DataSource source_name property"""
        source = ConcreteDataSource()
        assert source.source_name == "TestSource"


class TestPipeline:
    """Test Pipeline orchestrator"""

    def test_pipeline_initialization(self):
        """Test Pipeline initialization"""
        pipeline = Pipeline("Test Pipeline")
        assert pipeline.name == "Test Pipeline"
        assert len(pipeline.stages) == 0

    def test_pipeline_add_component(self):
        """Test adding component to pipeline"""
        pipeline = Pipeline("Test")
        component = ConcreteETLComponent()
        result = pipeline.add(component)
        
        assert len(pipeline.stages) == 1
        assert pipeline.stages[0] == component
        # Test fluent interface returns self
        assert result is pipeline

    def test_pipeline_add_multiple_components(self):
        """Test adding multiple components"""
        pipeline = Pipeline("Test")
        comp1 = ConcreteExtractor()
        comp2 = ConcreteTransformer()
        comp3 = ConcreteLoader()
        
        pipeline.add(comp1).add(comp2).add(comp3)
        assert len(pipeline.stages) == 3

    def test_pipeline_execute_all_succeed(self):
        """Test pipeline execution when all stages succeed"""
        pipeline = Pipeline("Test")
        pipeline.add(ConcreteExtractor(should_succeed=True))
        pipeline.add(ConcreteTransformer(should_succeed=True))
        pipeline.add(ConcreteLoader(should_succeed=True))
        
        result = pipeline.execute()
        assert result is True

    def test_pipeline_execute_one_fails(self):
        """Test pipeline stops when one stage fails"""
        pipeline = Pipeline("Test")
        pipeline.add(ConcreteExtractor(should_succeed=True))
        pipeline.add(ConcreteTransformer(should_succeed=False))
        pipeline.add(ConcreteLoader(should_succeed=True))
        
        result = pipeline.execute()
        assert result is False

    def test_pipeline_execute_first_fails(self):
        """Test pipeline fails on first stage failure"""
        pipeline = Pipeline("Test")
        pipeline.add(ConcreteExtractor(should_succeed=False))
        pipeline.add(ConcreteTransformer(should_succeed=True))
        
        result = pipeline.execute()
        assert result is False

    def test_pipeline_execute_raises_exception(self):
        """Test pipeline handles exception in stage"""
        pipeline = Pipeline("Test")
        component = ConcreteExtractor()
        
        with patch.object(component, 'execute', side_effect=RuntimeError("Stage error")):
            pipeline.add(component)
            result = pipeline.execute()
            assert result is False

    def test_pipeline_repr(self):
        """Test pipeline string representation"""
        pipeline = Pipeline("Test Pipeline")
        pipeline.add(ConcreteExtractor())
        pipeline.add(ConcreteTransformer())
        
        repr_str = repr(pipeline)
        assert "Test Pipeline" in repr_str
        assert "stages=2" in repr_str

    def test_pipeline_empty_execution(self):
        """Test executing empty pipeline"""
        pipeline = Pipeline("Empty")
        result = pipeline.execute()
        assert result is True

    def test_pipeline_single_stage_success(self):
        """Test pipeline with single successful stage"""
        pipeline = Pipeline("Single")
        pipeline.add(ConcreteExtractor(should_succeed=True))
        
        result = pipeline.execute()
        assert result is True

    def test_pipeline_single_stage_failure(self):
        """Test pipeline with single failed stage"""
        pipeline = Pipeline("Single")
        pipeline.add(ConcreteExtractor(should_succeed=False))
        
        result = pipeline.execute()
        assert result is False
