package org.otpstudy.observer

import org.otpstudy.supervisor.SupervisorChildInfo
import org.otpstudy.supervisor.SupervisorRef

/**
 * ASCII / DOT rendering for a static [SupervisorRef] using [SupervisorRef.whichChildren].
 *
 * Default [fromSupervisorRef] is one level. For nested supervisors, pass [nestedSupervisors]: a map
 * from **child id** (as in [SupervisorChildInfo.id]) to that child’s [SupervisorRef] so their
 * [SupervisorRef.whichChildren] are merged under that row (OTP-style nested tree for visualization).
 */
object SupervisorTree {

    data class Node(
        val label: String,
        val detail: String?,
        val children: List<Node> = emptyList(),
    )

    fun fromSupervisorRef(ref: SupervisorRef, name: String = "supervisor"): Node =
        fromSupervisorRef(ref, name, nestedSupervisors = emptyMap())

    fun fromSupervisorRef(
        ref: SupervisorRef,
        name: String = "supervisor",
        nestedSupervisors: Map<String, SupervisorRef>,
    ): Node {
        val kids =
            ref.whichChildren().map { c ->
                val detail = childDetail(c)
                val nested = nestedSupervisors[c.id]
                if (nested != null) {
                    val inner = fromSupervisorRef(nested, c.id, nestedSupervisors)
                    Node(c.id, detail, inner.children)
                } else {
                    Node(c.id, detail)
                }
            }
        return Node(name, null, kids)
    }

    private fun childDetail(c: SupervisorChildInfo): String {
        val extra = ProcessTable.all().firstOrNull { it.name == c.id }
        return buildString {
            append(c.childType)
            append(" active=")
            append(c.isActive)
            append(" restarts=")
            append(c.restartCount)
            if (extra != null) {
                append(" q=")
                append(extra.messageQueueLen)
                append(" mod=")
                append(extra.module)
            }
        }
    }

    fun toAscii(node: Node, prefix: String = "", isLast: Boolean = true): String = buildString {
        val branch = if (isLast) "└── " else "├── "
        val cont = if (isLast) "    " else "│   "
        val line = buildString {
            append(prefix)
            append(branch)
            append(node.label)
            node.detail?.let { append(" <").append(it).append(">") }
        }
        appendLine(line)
        node.children.forEachIndexed { i, child ->
            append(toAscii(child, prefix + cont, i == node.children.lastIndex))
        }
    }

    fun toDot(root: Node): String = buildString {
        appendLine("digraph supervision_tree {")
        appendLine("  rankdir=TB; node [fontname=\"monospace\" fontsize=10];")
        var seq = 0
        fun nextId() = "n${seq++}"
        fun visit(parentId: String?, n: Node): String {
            val id = nextId()
            val esc = n.label.replace("\"", "\\\"")
            val det = n.detail?.replace("\"", "\\\"") ?: ""
            val label = if (det.isEmpty()) esc else "$esc\\n$det"
            appendLine("  $id [label=\"$label\"];")
            parentId?.let { appendLine("  $it -> $id;") }
            n.children.forEach { visit(id, it) }
            return id
        }
        visit(null, root)
        appendLine("}")
    }
}
