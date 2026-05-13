import org.jetbrains.kotlin.backend.common.extensions.IrPluginContext
import org.jetbrains.kotlin.ir.UNDEFINED_OFFSET
import org.jetbrains.kotlin.ir.IrStatement
import org.jetbrains.kotlin.ir.declarations.IrFunction
import org.jetbrains.kotlin.ir.expressions.IrConstKind
import org.jetbrains.kotlin.ir.expressions.IrExpression
import org.jetbrains.kotlin.ir.expressions.IrLoop
import org.jetbrains.kotlin.ir.expressions.impl.IrBlockImpl
import org.jetbrains.kotlin.ir.expressions.impl.IrCallImplWithShape
import org.jetbrains.kotlin.ir.expressions.impl.IrConstImpl
import org.jetbrains.kotlin.ir.symbols.IrSimpleFunctionSymbol
import org.jetbrains.kotlin.ir.visitors.IrElementTransformerVoid
import org.jetbrains.kotlin.name.CallableId
import org.jetbrains.kotlin.name.FqName
import org.jetbrains.kotlin.name.Name

/**
 * Injects `reduce(1)` at every loop back-edge inside functions visible to the plugin.
 *
 * OTP source: erts/emulator/beam/erl_process.c — process_main, the reduction deduction loop
 */
class ReductionInjectionTransformer(
    private val ctx: IrPluginContext,
    private val autoInjectPackages: List<String>,
) : IrElementTransformerVoid() {

    private val reduceSymbol: IrSimpleFunctionSymbol? by lazy {
        ctx.referenceFunctions(
            CallableId(FqName("org.otpstudy.genserver"), Name.identifier("reduce"))
        ).firstOrNull()
    }

    private var currentFunction: IrFunction? = null

    override fun visitFunction(declaration: IrFunction): IrStatement {
        val prev = currentFunction
        currentFunction = declaration
        val result = super.visitFunction(declaration)
        currentFunction = prev
        return result
    }

    override fun visitLoop(loop: IrLoop): IrExpression {
        val transformed = super.visitLoop(loop) as IrLoop
        val sym = reduceSymbol ?: return transformed

        val intConst = IrConstImpl(
            UNDEFINED_OFFSET, UNDEFINED_OFFSET,
            ctx.irBuiltIns.intType,
            IrConstKind.Int,
            1,
        )
        // IrCallImplWithShape is the non-deprecated factory (replaces IrCallImpl in 2.2+).
        // reduce(n: Int) has 1 value arg, no receivers, no type args.
        val reduceCall = IrCallImplWithShape(
            UNDEFINED_OFFSET, UNDEFINED_OFFSET,
            ctx.irBuiltIns.unitType,
            sym,
            0,      // typeArgumentsCount
            1,      // valueArgumentsCount
            0,      // contextParameterCount
            false,  // hasDispatchReceiver
            false,  // hasExtensionReceiver
            null,   // origin
            null,   // superQualifierSymbol
        )
        // arguments[i] = expr is the non-deprecated replacement for putValueArgument(i, expr).
        reduceCall.arguments[0] = intConst

        val newBody = IrBlockImpl(
            UNDEFINED_OFFSET, UNDEFINED_OFFSET,
            ctx.irBuiltIns.unitType,
            null,
        )
        transformed.body?.let { newBody.statements.add(it) }
        newBody.statements.add(reduceCall)

        transformed.body = newBody
        return transformed
    }
}
