import * as React from 'react'
import { last } from '../charts/Util'
import { TextField } from './Forms'
import ColorSchemes, { ColorScheme } from '../charts/ColorSchemes'
import {action} from 'mobx'

export interface ColorpickerProps {
    color?: string
    onColor: (color: string | undefined) => void
    onClose: () => void
}

export default class Colorpicker extends React.Component<ColorpickerProps> {
    base!: HTMLDivElement

    componentDidMount() {
        const textField = this.base.querySelector("input") as HTMLInputElement
        textField.focus()

        setTimeout(() => window.addEventListener('click', this.onClickOutside), 10)
    }

    componentWillUnmount() {
        window.removeEventListener('click', this.onClickOutside)
    }

    @action.bound onClickOutside() {
        this.props.onClose()
    }

    @action.bound onColor(color: string) {
        if (color === "") {
            this.props.onColor(undefined)
        } else {
            this.props.onColor(color)
        }
    }

    render() {
        const availableColors: string[] = last((ColorSchemes['owid-distinct'] as ColorScheme).colorSets)

        return <div className="Colorpicker" tabIndex={0} onClick={e => e.stopPropagation()}>
            <ul>
                {availableColors.map(color =>
                    <li style={{ backgroundColor: color }} onClick={() => { this.props.onColor(color); this.props.onClose() }} />
                )}
            </ul>
            <TextField placeholder="#xxxxxx" value={this.props.color} onValue={this.onColor} onEnter={this.props.onClose} onEscape={this.props.onClose} />
        </div>
    }
}
