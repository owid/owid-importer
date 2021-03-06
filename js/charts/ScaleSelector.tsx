/* ScaleSelector.jsx
 * ================
 *
 * Small toggle component for switching between log/linear (or any other) scale types.
 *
 * @project Our World In Data
 * @author  Jaiden Mispy
 * @created 2017-02-11
 */

import * as React from 'react'
import { computed, action } from 'mobx'
import { observer } from 'mobx-react'
import ScaleType from './ScaleType'

interface ScaleSelectorProps {
    x: number
    y: number
    scaleType: ScaleType
    scaleTypeOptions: ScaleType[]
    onChange: (scaleType: ScaleType) => void
}

@observer
export default class ScaleSelector extends React.Component<ScaleSelectorProps> {
    @computed get x(): number { return this.props.x }
    @computed get y(): number { return this.props.y }

    @computed get scaleTypeOptions(): ScaleType[] {
        return this.props.scaleTypeOptions
    }

    @computed get scaleType(): ScaleType {
        return this.props.scaleType
    }

    @action.bound onClick() {
        const { scaleType, scaleTypeOptions } = this

        let nextScaleTypeIndex = scaleTypeOptions.indexOf(scaleType) + 1
        if (nextScaleTypeIndex >= scaleTypeOptions.length)
            nextScaleTypeIndex = 0

        this.props.onChange(scaleTypeOptions[nextScaleTypeIndex])
    }

    render() {
        const { x, y, onClick, scaleType } = this

        if (this.context.isStatic)
            return null

        const style = { 'font-size': '12px', 'text-transform': 'uppercase', 'cursor': 'pointer' }
        return <text x={x} y={y} onClick={onClick} style={style} className="clickable">
            <tspan style={{ fontFamily: "FontAwesome" }}>{'\uf013'}</tspan> {scaleType}
        </text>
    }
}
