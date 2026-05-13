import org.jetbrains.kotlin.backend.common.extensions.IrGenerationExtension
import org.jetbrains.kotlin.compiler.plugin.CompilerPluginRegistrar
import org.jetbrains.kotlin.compiler.plugin.ExperimentalCompilerApi
import org.jetbrains.kotlin.config.CompilerConfiguration
import org.jetbrains.kotlin.config.CompilerConfigurationKey

val AUTO_INJECT_PACKAGES_KEY = CompilerConfigurationKey<List<String>>("autoInjectPackages")

@OptIn(ExperimentalCompilerApi::class)
class ReductionPluginRegistrar : CompilerPluginRegistrar() {
    override val supportsK2: Boolean get() = true

    override fun ExtensionStorage.registerExtensions(configuration: CompilerConfiguration) {
        val packages = configuration.getList(AUTO_INJECT_PACKAGES_KEY)
        IrGenerationExtension.registerExtension(ReductionInjectionExtension(packages))
    }
}
