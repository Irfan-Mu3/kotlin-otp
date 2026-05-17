package org.otpstudy.distribution

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

@Serializable
sealed class DistMsg {
    @Serializable
    @SerialName("hello")
    data class Hello(val node: String, val digest: String) : DistMsg()

    @Serializable
    @SerialName("cast")
    data class Cast(val target: String, val msg: JsonElement) : DistMsg()

    @Serializable
    @SerialName("call")
    data class Call(val target: String, val id: String, val req: JsonElement) : DistMsg()

    @Serializable
    @SerialName("reply")
    data class Reply(val id: String, val result: JsonElement) : DistMsg()

    @Serializable
    @SerialName("ping")
    data object Ping : DistMsg()

    @Serializable
    @SerialName("pong")
    data object Pong : DistMsg()

    /** Global name replication ([GlobalDistMsg]) — not routed to a named actor. */
    @Serializable
    @SerialName("global")
    data class Global(val payload: GlobalDistMsg) : DistMsg()
}
