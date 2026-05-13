import org.jetbrains.kotlin.backend.common.extensions.IrGenerationExtension
import org.jetbrains.kotlin.backend.common.extensions.IrPluginContext
import org.jetbrains.kotlin.ir.declarations.IrModuleFragment

class ReductionInjectionExtension(
    private val autoInjectPackages: List<String>,
) : IrGenerationExtension {
    override fun generate(moduleFragment: IrModuleFragment, pluginContext: IrPluginContext) {
        val transformer = ReductionInjectionTransformer(pluginContext, autoInjectPackages)
        moduleFragment.transform(transformer, null)
    }
}
