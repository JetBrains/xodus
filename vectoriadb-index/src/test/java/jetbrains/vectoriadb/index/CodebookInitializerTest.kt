package jetbrains.vectoriadb.index

import org.junit.Assert
import org.junit.Test
import kotlin.math.min

class CodebookInitializerTest {

    @Test
    fun getCodebookCount() {
        val vectorDimension = 128

        Assert.assertEquals(16, CodebookInitializer.getCodebookCount(vectorDimension, 32))
        Assert.assertEquals(12, CodebookInitializer.getCodebookCount(vectorDimension, 40))
        Assert.assertEquals(8, CodebookInitializer.getCodebookCount(vectorDimension, 64))
        Assert.assertEquals(5, CodebookInitializer.getCodebookCount(vectorDimension, 100))
        Assert.assertEquals(4, CodebookInitializer.getCodebookCount(vectorDimension, 128))
    }

    @Test
    fun initializeCodebooks() {
        listOf(
            TestCase(codebookCount = 15, vectorCount = 1000, vectorDimension = 256),
            TestCase(codebookCount = 15, vectorCount = 30, vectorDimension = 256),
            TestCase(codebookCount = 15, vectorCount = 1000, vectorDimension = 15),
        ).forEach { (codebookCount, vectorCount, vectorDimensions) ->
            val initializer = CodebookInitializer(codebookCount, vectorCount, vectorDimensions)

            assert(initializer.codeBaseSize == min(CodebookInitializer.CODE_BASE_SIZE, vectorCount))

            with(initializer.codebookDimensions) {
                assert(size == codebookCount)
                assert(sum() == vectorDimensions)
                val minDimensions = min()
                val maxDimensions = max()
                assert(minDimensions == maxDimensions || minDimensions + 1 == maxDimensions)
                assert(initializer.maxCodebookDimensions == maxDimensions)
            }

            with(initializer.codebooks) {
                assert(size == codebookCount)
                assert(first().size == initializer.codeBaseSize)
                assert(first().first().size == initializer.maxCodebookDimensions)
                assert(last().first().size == initializer.maxCodebookDimensions)
            }


            with(initializer.codebookDimensionOffset) {
                assert(size == codebookCount)
                var expectedOffset = 0
                for (codebookIdx in 0 until codebookCount) {
                    if (codebookIdx > 0) {
                        expectedOffset += initializer.codebookDimensions[codebookIdx - 1]
                    }
                    assert(get(codebookIdx) == expectedOffset)
                }
            }
        }

    }

    private data class TestCase(val codebookCount: Int, val vectorCount: Int, val vectorDimension: Int)
}