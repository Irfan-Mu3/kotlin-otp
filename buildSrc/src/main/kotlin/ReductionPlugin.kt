import org.gradle.api.Project
import org.gradle.api.provider.Provider
import org.jetbrains.kotlin.gradle.plugin.KotlinCompilation
import org.jetbrains.kotlin.gradle.plugin.KotlinCompilerPluginSupportPlugin
import org.jetbrains.kotlin.gradle.plugin.SubpluginArtifact
import org.jetbrains.kotlin.gradle.plugin.SubpluginOption

open class OtpReductionsExtension {
    var autoInjectPackages: List<String> = emptyList()
}

class ReductionPlugin : KotlinCompilerPluginSupportPlugin {
    override fun apply(target: Project) {
        target.extensions.create("otpReductions", OtpReductionsExtension::class.java)
    }

    override fun isApplicable(kotlinCompilation: KotlinCompilation<*>): Boolean = true

    override fun getCompilerPluginId(): String = "org.otpstudy.reduction-plugin"

    override fun getPluginArtifact(): SubpluginArtifact = SubpluginArtifact(
        groupId = "org.otpstudy",
        artifactId = "reduction-plugin",
        version = "0.1.0-SNAPSHOT",
    )

    override fun applyToCompilation(
        kotlinCompilation: KotlinCompilation<*>
    ): Provider<List<SubpluginOption>> {
        val project = kotlinCompilation.target.project
        val ext = project.extensions.findByType(OtpReductionsExtension::class.java)
        val packages = ext?.autoInjectPackages?.joinToString(",") ?: ""
        return project.provider {
            listOf(SubpluginOption(key = "autoInjectPackages", value = packages))
        }
    }
}
